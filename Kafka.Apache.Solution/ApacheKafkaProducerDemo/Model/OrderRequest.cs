﻿namespace ApacheKafkaProducerDemo.Model
{
    public class OrderRequest
    {
        public int OrderId { get; set; }
        public int ProductId { get; set; }
        public int CustomerId { get; set; }
        public int Quantity { get; set; }
        public int Status { get; set; }
    }
}
