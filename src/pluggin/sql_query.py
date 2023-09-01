def vm_pubsub_3M_query(max_date,row_index,limit_max,limit_min):
    query =f"""    
                WITH cte_daily_amount AS     (
                        SELECT  
                                CAST(time AS DATE)  AS Time,
                                ticker,
                                SUM(close) AS price_close
                        FROM
                                stock_info
                        GROUP BY Time ,ticker

                        HAVING
                                CAST(time AS DATE) BETWEEN DATE_SUB(CAST('{str(max_date)}' AS DATE) , 90) AND CAST('{str(max_date)}' AS DATE)
                                )
                SELECT  Time,
                        ticker,
                        price_close,
                        previous_date_price,
                        percentage_change_close,
                        MA10,
                        percentage_change_MA10,
                        sum_percentage_change_close,
                        sum_percentage_change_MA10
                FROM(
                        
                SELECT  Time,
                        ticker,
                        price_close,
                        previous_date_price,
                        percentage_change_close,
                        MA10,
                        percentage_change_MA10,
                        ROUND((SUM(percentage_change_close) OVER (PARTITION BY ticker)),2) AS sum_percentage_change_close,
                        ROUND((SUM(percentage_change_MA10) OVER (PARTITION BY ticker)),2) AS sum_percentage_change_MA10

                FROM(
                SELECT    Time,ticker,price_close,MA10,percentage_change_MA10,
                        MAX(percentage_change_MA10) OVER (PARTITION BY ticker) AS max_percentage_change,
                        previous_date_price,
                        ROUND(((price_close-previous_date_price)*100/previous_date_price),2) AS percentage_change_close,

                        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time) AS row_num
                FROM (
                SELECT  Time,
                        ticker,
                        price_close,
                        CASE 
                                WHEN ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY Time) > {row_index} 
                                THEN ROUND(AVG(price_close) OVER(PARTITION BY ticker 
                                        ORDER BY Time ROWS BETWEEN {row_index} PRECEDING AND CURRENT ROW),2)
                                ELSE NULL 
                        END AS MA10,
                        LAG(price_close, -1) OVER (PARTITION BY ticker ORDER BY time DESC) AS previous_date_price,

                        ROUND(((price_close-MA10)*100/MA10),2) AS percentage_change_MA10
                FROM
                        cte_daily_amount  )
                AS percentage_change_MA10
                ) AS max_percentage_change
                WHERE max_percentage_change <= {limit_max} AND max_percentage_change >= {limit_min} AND row_num > 10
                ) AS sum_percentage_change_close
                WHERE sum_percentage_change_close > 0
                ;
        """
    return query
def vm_pubsub_1Y_query():
    query=f"""SELECT      time,
                        ticker,
                        type,
                        close,
                        open,
                        high,
                        low,
                        volume,
                        ROUND(((close-previous_date_price)*100/previous_date_price),2) AS percentage_change
                        FROM 
                        (
                        SELECT
                        time,
                        type,
                        ticker,
                        close,
                        open,
                        high,
                        low,
                        volume,
                        LAG(close,-1) OVER (
                                PARTITION BY ticker
                                ORDER BY time DESC
                        ) AS previous_date_price
                        FROM stock_info
                        ) AS result
                        
        """
    return query