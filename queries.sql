
-- Qual o total de transações aprovadas por mês?

SELECT

  DATE_TRUNC(transaction_date, MONTH) AS month
  ,COUNT(*) total_transactions

FROM `teste-eng-dados.dataset_teste.transactions`

WHERE 1=1
  AND transaction_status = "approved"

GROUP BY ALL

ORDER BY month;


-- Qual cliente teve o maior volume de transações aprovadas nos últimos 3 meses?

SELECT

  customer_name
  ,COUNT(*) total_transactions

FROM `teste-eng-dados.dataset_teste.transactions` tr
,`teste-eng-dados.dataset_teste.customers` c

WHERE 1=1
  AND tr.customer_id = c.customer_id
  AND transaction_status = "approved"
  AND transaction_date BETWEEN "2023-10-01" AND "2023-12-31"

GROUP BY ALL

ORDER BY total_transactions DESC

LIMIT 1;


-- Qual a média de transações rejeitadas por mês no último ano?

SELECT

  COUNT(CASE WHEN transaction_status = "rejected" THEN transaction_id END) AS total_rejected
  ,COUNT(DISTINCT DATE_TRUNC(transaction_date, MONTH)) AS total_months
  ,ROUND(COUNT(CASE WHEN transaction_status = "rejected" THEN transaction_id END) / COUNT(DISTINCT DATE_TRUNC(transaction_date, MONTH)), 2) AS avg_rejected_per_month

FROM `teste-eng-dados.dataset_teste.transactions`;


-- Qual o preço médio do estoque do ativo em questão, desconsiderando a
-- abertura de clientes, e como ele se comporta conforme as transações são
-- realizadas.

# Considerando o estoque como total de compra - total de venda
# A média do valor do estoque seria (valor total compra - valor total venda) / (qtty compra - qtty venda)
# Para verificar esse comportamento conforme as transações são realizadas, agrupou-se por dia

SELECT 

  transaction_date
  ,ROUND((
      SUM(CASE WHEN transaction_type = 'buy' THEN qtty * price ELSE 0 END)
      -SUM(CASE WHEN transaction_type = 'sell' THEN qtty * price ELSE 0 END)
    ) 
    / 
    (
      SUM(CASE WHEN transaction_type = 'buy' THEN qtty ELSE 0 END)
      -SUM(CASE WHEN transaction_type = 'sell' THEN qtty ELSE 0 END)
    ), 2) AS stock_price

FROM `teste-eng-dados.dataset_teste.transactions`

WHERE 1=1
  AND transaction_status = "approved"

GROUP BY ALL

ORDER BY transaction_date;
