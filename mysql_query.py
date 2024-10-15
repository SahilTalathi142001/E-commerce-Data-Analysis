import pandas as pd
import mysql.connector

conn = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="username",
            password="password",
            database="database_name"
)

cursor = conn.cursor()

#List all unique cities where customers are located.
query = """ select distinct customer_city from customers """
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns=['customer_city'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/unique_cities.csv', index=False,header=True)
print(df)

#Count the number of orders placed in 2017.
query=""" select count(order_id) as Orders_placed from orders where order_purchase_timestamp like '2017%' """
cursor.execute(query)
data=cursor.fetchall()
df=pd.DataFrame(data,columns=['Orders placed'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/number of orders placed in 2017.csv',index=False,header=True)
print(df)


#Find the total sales per category
query=""" select p.product_category as category, round(sum(pay.payment_value),2) as sales from order_items oi
join products p on oi.product_id=p.product_id join payments as pay on pay.order_id=oi.order_id
group by p.product_category """
cursor.execute(query)
data=cursor.fetchall()
df=pd.DataFrame(data,columns=['category','Sales'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/total sales per category.csv',index=False,header=True)
print(df)

#Calculate the percentage of orders that were paid in installments
query=""" select (count(distinct o.order_id)/(SELECT COUNT(*) FROM orders) * 100) as order_percent from orders o
join payments pay
on o.order_id=pay.order_id where pay.payment_installments>=1 """
cursor.execute(query)
data=cursor.fetchall()
df=pd.DataFrame(data,columns=['order_percent'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/percentage of orders that were paid in installments.csv',index=False,header=True)
print(df)

#Count the number of customers from each state.
query='''select c.customer_state,count(c.customer_id) as Count_of_Customers from customers c group by
c.customer_state'''
cursor.execute(query)
data=cursor.fetchall()
df=pd.DataFrame(data,columns=['Customer State','Count of Customers'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/number of customers from each state.csv',index=False,header=True)
print(df)

#Calculate the number of orders per month in 2018.
query=''' select count(o.order_id) as Count_of_orders from orders o where year(order_purchase_timestamp)=2018 group by monthname(order_purchase_timestamp)  '''
cursor.execute(query)
data=cursor.fetchall()
order=["January", "February","March","April","May","June","July","August","September","October"]
df=pd.DataFrame(data,columns=['Count of Orders'],index=order)
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/number of orders per month in 2018.csv',index=True,header=True)
print(df)

#Find the average number of products per order, grouped by customer city.
query=''' with count_per_order as
(select orders.order_id, orders.customer_id, count(order_items.order_id) as oc
from orders join order_items
on orders.order_id = order_items.order_id
group by orders.order_id, orders.customer_id)

select customers.customer_city, round(avg(count_per_order.oc),2) average_orders
from customers join count_per_order
on customers.customer_id = count_per_order.customer_id
group by customers.customer_city order by average_orders desc '''
cursor.execute(query)
data=cursor.fetchall()
df=pd.DataFrame(data,columns=['Customer City','Avg of Product per order'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/average number of products per order by customer city.csv',index=False,header=True)
print(df)

#Calculate the percentage of total revenue contributed by each product category.
query = """select upper(products.product_category) category,
round((sum(payments.payment_value)/(select sum(payment_value) from payments))*100,2) sales_percentage
from products join order_items
on products.product_id = order_items.product_id
join payments
on payments.order_id = order_items.order_id
group by category order by sales_percentage desc"""


cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns = ["Category", "percentage distribution"])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/percentage of total revenue contributed by each product category.csv',header=True,index=False)
print(df)

#Identify the correlation between product price and the number of times a product has been purchased.
query = """select products.product_category,
count(order_items.product_id),
round(avg(order_items.price),2)
from products join order_items
on products.product_id = order_items.product_id
group by products.product_category"""

cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns = ["Category", "order_count","price"])

arr1 = df["order_count"]
arr2 = df["price"]
import numpy as np
a = np.corrcoef([arr1,arr2])
print("the correlation is", a[0][1])

#Calculate the total revenue generated by each seller, and rank them by revenue.
query='''with tb as (
select sum(pay.payment_value) as payments, oi.seller_id from payments pay join order_items oi on
pay.order_id=oi.order_id group by seller_id
)
select dense_rank() over(order by payments desc) as rnk,seller_id,payments from tb
'''
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns = ['rnk',"seller_id","payments"])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/total revenue generated by each seller and rank them by revenue.csv',index=False,header=True)
print(df)

#Calculate the moving average of order values for each customer over their order history.
query=''' select o.customer_id,avg(pay.payment_value) over(partition by o.customer_id order by
o.order_purchase_timestamp)
as avg_order from orders o join payments pay on pay.order_id=o.order_id '''
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns = ['customer id',"avg"])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/moving average of order values for each customer over their order history.csv',header=True,index=False)
print(df)

#Calculate the cumulative sales per month for each year.
query='''
with tb as(
select month(o.order_purchase_timestamp) as month,year(o.order_purchase_timestamp) as year,
sum(pay.payment_value) over(partition by YEAR(o.order_purchase_timestamp)
order by year(o.order_purchase_timestamp),month(o.order_purchase_timestamp)) as sales from orders o join payments pay on
o.order_id=pay.order_id
)
select month,year,round(sales,2) from tb
group by year,month,sales
ORDER BY year, month'''

cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns = ['month','year','sales'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/cumulative sales per month for each year.csv',header=True,index=False)
print(df)

#Calculate the year-over-year growth rate of total sales

query=''' with a as(select year(orders.order_purchase_timestamp) as years,
round(sum(payments.payment_value),2) as payment from orders join payments
on orders.order_id = payments.order_id
group by years order by years)

select years, ((payment - lag(payment, 1) over(order by years))/
lag(payment, 1) over(order by years)) * 100 from a
'''
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns = ['order year','growth rate'])
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/Calculate the year-over-year growth rate of total sales.csv',header=True,index=False)

#Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

query="""select years, customer_id, payment, d_rank
from
(select year(orders.order_purchase_timestamp) years,
orders.customer_id,
sum(payments.payment_value) payment,
dense_rank() over(partition by year(orders.order_purchase_timestamp)
order by sum(payments.payment_value) desc) d_rank
from orders join payments
on payments.order_id = orders.order_id
group by year(orders.order_purchase_timestamp),
orders.customer_id) as a
where d_rank <= 3 """
cursor.execute(query)
data = cursor.fetchall()
df = pd.DataFrame(data,columns = ["years","id","payment","rank"])
print(df)
df.to_csv('C:/Users/Sagar/OneDrive/Desktop/ecommerce/payment rank.csv',header=True,index=False)

cursor.close()
conn.close()