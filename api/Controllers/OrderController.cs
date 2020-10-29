using System;
using System.Threading.Tasks;
using api.Models;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace api.Controllers
{
    [Route("api/[controller]")]
    public class OrderController : ControllerBase
    {
       private readonly ProducerConfig config; 

       public OrderController(ProducerConfig config)
       {
           this.config = config;
       }

       public async Task<ActionResult> PostAsync([FromBody]OrderRequest value)
       {
           if(!ModelState.IsValid)
           {
               return BadRequest(ModelState);
           }

           var order = JsonConvert.SerializeObject(value);
           Console.WriteLine("=============");
           Console.WriteLine("Info: OrderController => Post => Received a new purchase order:");
           Console.WriteLine(order);
           Console.WriteLine("=============");

           var producer = new ProducerWrapper(this.config, "orderrequests");
           await producer.PublishMessage(order);

           return Created($"TransactionId", "Your order is in progress");
       }
       
    }
}