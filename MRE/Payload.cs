using System.Text;
using System.Text.Json;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;

public class Payload
{
    public static int MessagesSent = 0;
    public int Count { get; set; }

    public Message ToMessage()
    {
        return new Message(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this)))
        {
            Properties = new Properties
            {
                Subject = nameof(Payload)
            }
        };
    }
}
