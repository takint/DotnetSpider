using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using DotnetSpider.MessageQueue;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

[assembly: InternalsVisibleTo("DotnetSpider.Tests")]

namespace DotnetSpider.RabbitMQ;

public class RabbitMQMessageQueue : IMessageQueue, IAsyncDisposable
{
    private readonly RabbitMQOptions _options;
    private readonly PersistentConnection _connection;
    private readonly ILogger<RabbitMQMessageQueue> _logger;
    private IChannel _publishChannel;

    public RabbitMQMessageQueue(IOptions<RabbitMQOptions> options, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<RabbitMQMessageQueue>();
        _options = options.Value;
        _connection = new PersistentConnection(CreateConnectionFactory(),
            loggerFactory.CreateLogger<PersistentConnection>(), _options.RetryCount);
    }

    public static async Task<RabbitMQMessageQueue> CreateAsync(IOptions<RabbitMQOptions> options,
        ILoggerFactory loggerFactory)
    {
        var instance = new RabbitMQMessageQueue(options, loggerFactory);
        await instance.InitializeAsync();
        return instance;
    }

    private async Task InitializeAsync()
    {
        if (!_connection.IsConnected)
        {
            await _connection.TryConnectAsync();
        }

        _logger.LogTrace("Creating RabbitMQ publish channel");

        _publishChannel = await _connection.CreateChannelAsync();
        await _publishChannel.ExchangeDeclareAsync(_options.Exchange, "direct", durable: true);
    }

    private IConnectionFactory CreateConnectionFactory()
    {
        var connectionFactory = new ConnectionFactory
        {
            HostName = _options.HostName
        };

        if (_options.Port > 0)
        {
            connectionFactory.Port = _options.Port;
        }

        if (!string.IsNullOrWhiteSpace(_options.UserName))
        {
            connectionFactory.UserName = _options.UserName;
        }

        if (!string.IsNullOrWhiteSpace(_options.Password))
        {
            connectionFactory.Password = _options.Password;
        }

        return connectionFactory;
    }

    public async Task PublishAsync(string topic, byte[] bytes)
    {
        if (bytes == null)
        {
            throw new ArgumentNullException(nameof(bytes));
        }

        if (!_connection.IsConnected)
        {
            await _connection.TryConnectAsync();
        }

        var policy = Policy.Handle<BrokerUnreachableException>()
            .Or<SocketException>()
            .WaitAndRetryAsync(_options.RetryCount,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, time) =>
                {
                    _logger.LogWarning(ex, "Could not publish data after {Timeout}s",
                        $"{time.TotalSeconds:n1}");
                    return Task.CompletedTask;
                });

        await policy.ExecuteAsync(async () =>
        {
            var properties = new BasicProperties
            {
                DeliveryMode = DeliveryModes.Persistent,
                ContentEncoding = "lz4"
            };

            _logger.LogTrace("Publishing event to RabbitMQ");

            await _publishChannel.BasicPublishAsync(
                exchange: _options.Exchange,
                routingKey: topic,
                mandatory: true,
                basicProperties: properties,
                body: bytes);
        });
    }

    public async Task CloseQueueAsync(string queue)
    {
        var channel = await _connection.CreateChannelAsync();
        await using (channel)
        {
            await channel.QueueDeleteAsync(queue);
        }
    }

    // Keep sync version for interface compatibility if needed
    public void CloseQueue(string queue) =>
        CloseQueueAsync(queue).GetAwaiter().GetResult();

    public bool IsDistributed => true;

    public async Task ConsumeAsync(AsyncMessageConsumer<byte[]> consumer)
    {
        if (consumer.Registered)
        {
            throw new ApplicationException("This consumer is already registered");
        }

        if (!_connection.IsConnected)
        {
            await _connection.TryConnectAsync();
        }

        var channel = await _connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: consumer.Queue,
            durable: true,
            exclusive: false,
            autoDelete: true,
            arguments: null);

        await channel.QueueBindAsync(consumer.Queue, _options.Exchange, consumer.Queue);

        var basicConsumer = new AsyncEventingBasicConsumer(channel);

        basicConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                await consumer.InvokeAsync(ea.Body.ToArray());
            }
            finally
            {
                await channel.BasicAckAsync(ea.DeliveryTag, false);
            }
        };

        consumer.OnClosing += async x =>
        {
            await channel.CloseAsync();
        };

        await channel.BasicConsumeAsync(consumer.Queue, false, basicConsumer);
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        if (_publishChannel != null)
        {
            await _publishChannel.DisposeAsync();
        }

        if (_connection != null)
        {
            await _connection.DisposeAsync();
        }
    }
}
