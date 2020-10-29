using System;
using System.Threading;
using System.Threading.Tasks;
using api.Models;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;

namespace api.Services
{
    public class ProcessOrdersService : BackgroundService
    {
        private readonly ConsumerConfig consumerConfig;
        private readonly ProducerConfig producerConfig;
        public ProcessOrdersService(ConsumerConfig consumerConfig, ProducerConfig producerConfig)
        {
            this.producerConfig = producerConfig;
            this.consumerConfig = consumerConfig;
        }  

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("OrderProcessing Service Started");
            
            while (!cancellationToken.IsCancellationRequested)
            {
                using(var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
                {
                    consumer.Subscribe("orderrequests");

                    while(!cancellationToken.IsCancellationRequested)
                    {
                        var result = consumer.Consume(cancellationToken);
                        // handle the message
                        var order = JsonConvert.DeserializeObject<OrderRequest>(result.Message.Value);

                        Console.WriteLine($"Info: OrderHandler => Processing the order for {order.productname}");
                        order.status = OrderStatus.COMPLETED;

                        var producerWrapper = new ProducerWrapper(producerConfig,"readytoship");
                        await producerWrapper.PublishMessage(JsonConvert.SerializeObject(order));
                    }

                    consumer.Close();
                }
            }
        }              
    }
}