using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace WorkService.Proposta
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
                var factory = new ConnectionFactory { Uri = new Uri($"amqp://guest:guest@localhost:5672/") };
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    // Declaração da fila principal
                    channel.QueueDeclare(queue: "SolicitarCartao", durable: true, exclusive: false, autoDelete: false, arguments: null);

                    // Configuração da Dead-Letter Exchange
                    var dlxName = "SolicitarCartao-dlx";
                    var queueDLX = "SolicitarCartao-fila-dlx";
                    var routykeyDLX = "SolicitarCartao-dlx-key";
                    channel.ExchangeDeclare(dlxName, ExchangeType.Fanout, durable: true);
                    channel.QueueDeclare(queue: queueDLX, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueBind(queueDLX, dlxName, routykeyDLX);
                while (!stoppingToken.IsCancellationRequested)
                {
                    // Configuração do consumidor       
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        _logger.LogInformation("Mensagem recebida: {0}", message);

                        // Aqui está um exemplo de como redirecionar a mensagem para a DLX
                        if (message.Contains("erro"))
                        {
                            _logger.LogInformation("Mensagem com error");
                            var props = ea.BasicProperties;
                            var headers = props.Headers ?? new Dictionary<string, object>();
                            headers.Add("x-death-reason", "Erro na mensagem");
                            headers.Add("x-death-count", 1);
                            props.Headers = headers;
                            channel.BasicPublish(dlxName, "", props, body);
                        }
                        else
                        {
                            _logger.LogInformation("Mensagem com Sucesso");
                            // Faz o processamento normal da mensagem
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                        }
                    };
                    channel.BasicConsume(queue: "SolicitarCartao", autoAck: false, consumer: consumer);
                    await Task.Delay(1_000, stoppingToken);
                }
                
                }
        }
    }
}