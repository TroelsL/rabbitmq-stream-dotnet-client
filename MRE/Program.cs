using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

const string StreamName = "TEST_STREAM";

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.SetMinimumLevel(LogLevel.Debug)
    .AddDebug();
});

var outerLogger = loggerFactory.CreateLogger<Program>();

var config = new StreamSystemConfig
{
    Password = "password",
    UserName = "user",
    //Endpoints = new[] { new IPEndPoint(new IPAddress(new byte[] { 192, 168, 1, 9 }), 5552) }
    Endpoints = new[] { new DnsEndPoint("zen2", 5552) }
};

var system = await StreamSystem.Create(config)!.ConfigureAwait(false);

if (!await system.StreamExists(StreamName).ConfigureAwait(false))
{
    await system.CreateStream(new StreamSpec(StreamName)).ConfigureAwait(false);
}

var producerConfig = new ProducerConfig(system, StreamName)
{

};
var producer = await Producer.Create(producerConfig, loggerFactory.CreateLogger<Producer>()).ConfigureAwait(false);

var consumerConfig = new ConsumerConfig(system, StreamName)
{
    MessageHandler = MessageReceived
};

Task MessageReceived(string stream, RawConsumer consumer, MessageContext context, Message message)
{
    var payload = JsonSerializer.Deserialize<Payload>(Encoding.UTF8.GetString(message.Data.Contents))!;
    outerLogger.LogInformation("Received #{count}", payload.Count);
    return Task.CompletedTask;
}

var consumer = await Consumer.Create(consumerConfig, loggerFactory.CreateLogger<Consumer>()).ConfigureAwait(false);

await Task.Run(async () =>
{
    while (true)
    {
        var payload = new Payload() { Count = ++Payload.MessagesSent };
        outerLogger.LogInformation("Sent message #{count}. Consumer:{consumerIsOpen}. Producer:{producerIsOpen}", payload.Count, consumer.IsOpen(), producer.IsOpen());
        await producer.Send(payload.ToMessage()).ConfigureAwait(false);
        await Task.Delay(1000).ConfigureAwait(false);
    }
}).ConfigureAwait(false);

Console.ReadLine();
