using System;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace DotnetSpider.RabbitMQ;

public class PersistentConnection(
    IConnectionFactory connectionFactory,
    ILogger<PersistentConnection> logger,
    int retryCount = 5)
    : IAsyncDisposable, IDisposable
{
    private readonly IConnectionFactory _connectionFactory =
        connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));

    private readonly ILogger<PersistentConnection> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    private IConnection _connection;
    private bool _disposed;
    private readonly object _syncLocker = new();

    public bool IsConnected => _connection != null && _connection.IsOpen && !_disposed;

    public async Task<bool> TryConnectAsync()
    {
        _logger.LogInformation("RabbitMQ Client is trying to connect");

        var policy = Policy.Handle<SocketException>()
            .Or<BrokerUnreachableException>()
            .WaitAndRetryAsync(retryCount,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (ex, time) =>
                {
                    _logger.LogWarning(ex,
                        "RabbitMQ Client could not connect after {TimeOut}s",
                        $"{time.TotalSeconds:n1}");
                    return Task.CompletedTask;
                });

        await policy.ExecuteAsync(async () =>
        {
            _connection = await _connectionFactory.CreateConnectionAsync();
        });

        if (IsConnected)
        {
            _connection.ConnectionShutdownAsync += OnConnectionShutdownAsync;
            _connection.CallbackExceptionAsync += OnCallbackExceptionAsync;
            _connection.ConnectionBlockedAsync += OnConnectionBlockedAsync;

            _logger.LogInformation(
                "RabbitMQ Client acquired a persistent connection to '{HostName}' and is subscribed to failure events",
                _connection.Endpoint.HostName);

            return true;
        }

        _logger.LogCritical("FATAL ERROR: RabbitMQ connections could not be created and opened");
        return false;
    }

    public async Task<IChannel> CreateChannelAsync()
    {
        if (!IsConnected)
        {
            throw new InvalidOperationException("No RabbitMQ connections are available to perform this action");
        }

        return await _connection.CreateChannelAsync();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            _connection?.Dispose();
        }
        catch (IOException ex)
        {
            _logger.LogCritical(ex, "RabbitMQ 连接释放异常");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            if (_connection != null)
            {
                await _connection.DisposeAsync();
            }
        }
        catch (IOException ex)
        {
            _logger.LogCritical(ex, "RabbitMQ 连接释放异常");
        }
    }

    private async Task OnConnectionBlockedAsync(object sender, ConnectionBlockedEventArgs e)
    {
        if (_disposed) return;
        _logger.LogWarning("A RabbitMQ connection is blocked. Trying to re-connect...");
        await TryConnectAsync();
    }

    private async Task OnCallbackExceptionAsync(object sender, CallbackExceptionEventArgs e)
    {
        if (_disposed) return;
        _logger.LogWarning("A RabbitMQ connection threw exception. Trying to re-connect...");
        await TryConnectAsync();
    }

    private async Task OnConnectionShutdownAsync(object sender, ShutdownEventArgs reason)
    {
        if (_disposed) return;
        _logger.LogWarning("A RabbitMQ connection is shutting down. Trying to re-connect...");
        await TryConnectAsync();
    }
}
