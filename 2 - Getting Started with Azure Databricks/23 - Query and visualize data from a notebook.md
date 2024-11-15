# Query and visualize data from a notebook

``` sql
show schemas in samples;
```

``` sql
show tables in samples.bakehouse;
```

``` sql
select * from samples.bakehouse.sales_transactions;
```

``` sql
select product, sum(totalPrice)
from samples.bakehouse.sales_transactions
group by product;
```

``` sql
select count(*) from samples.bakehouse.sales_transactions
```