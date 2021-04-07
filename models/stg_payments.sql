with orders as (

    SELECT * FROM {{ ref('stg_orders') }}

),

payment as (
    SELECT * FROM raw.stripe.payment
),

payments as (
    SELECT 
    order_id,
    customer_id,
    SUM(amount)::FLOAT/100::FLOAT as payment
    FROM orders 
    JOIN payment 
    ON payment.orderid = orders.order_id
    WHERE payment.STATUS = 'success'
    GROUP BY order_id, customer_id
)

SELECT * FROM payments