# Spark-practice

A set of exercises running on this downloadable dataset:
https://drive.google.com/file/d/1kCXnIeoPT6p9kS_ANJ0mmpxlfDwK1yio/view?usp=sharing

Tables (represented as parquet files):
Products, sales, sellers

Questions being answered by the corresponding objects:

- Find out how many orders, how many products and how many sellers are in the data.
How many products have been sold at least once? Which is the product contained in more orders?

- How many distinct products have been sold in each day?

- What is the average revenue of the orders?

- For each seller, what is the average % contribution of an order to the seller's daily quota?
  Example
  If Seller_0 with `quota=250` has 3 orders:
  Order 1: 10 products sold
  Order 2: 8 products sold
  Order 3: 7 products sold
  The average % contribution of orders to the seller's quota would be:
  Order 1: 10/105 = 0.04
  Order 2: 8/105 = 0.032
  Order 3: 7/105 = 0.028
  Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333
  
 - Who are the second most selling and the least selling persons (sellers) for each product? Who are those for product with `product_id = 0`
 
 - Create a new column called "hashed_bill" defined as follows:
   - if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
   - if the order_id is odd: apply SHA256 hashing to the bill text
   Finally, check if there are any duplicate on the new column
   
   
 All those questions aim to get a grasp from basic to advanced concepts in Spark, starting from the very basics until more complex ones such as:
 - Joins Skewness: This is usually the main pain point in Spark pipelines; sometimes it is very difficult to solve, because it’s not easy to find a balance among all the factors that are involved in these operations.
 - Window functions: Super useful, the only thing to remember is to first define the windowing.
 - UDFs: Although they are very helpful, we should think twice before jumping into the development of such functions, since their execution might slow down our code.
 
PS: This is obviously not production code, a lot would have to be improved in terms of design and configuration.