# Case_Ecommerce
Projetos Case de Teste Ecommerce

# Projeto de Análise de Dados com Databricks

Este repositório contém o código para realizar uma análise de dados utilizando o Databricks, com foco em um banco de dados de e-commerce. A análise inclui a extração de dados de várias tabelas e a execução de consultas SQL para responder a perguntas específicas.

## Tabelas Utilizadas

As seguintes tabelas foram utilizadas na análise:

1. **customers**
2. **employees**
3. **offices**
4. **orderdetails**
5. **orders**
6. **payments**
7. **product_lines**
8. **products**


## Configuração do Ambiente

Para realizar a análise, siga as etapas abaixo:

1. **Criar um Notebook no Databricks**:
   - Crie um novo notebook no Databricks e selecione a linguagem Python.

2. **Conectar ao Banco de Dados**:
   - Utilize as credenciais do banco de dados para se conectar. Certifique-se de que a biblioteca PostgreSQL está instalada no seu cluster Databricks.

```python
# Exemplo de código para conexão
jdbc_url = "jdbc:postgresql://psql-mock-database-cloud.postgres.database.azure.com:5432/ecom1692155331663giqokzaqmuqlogbu"
connection_properties = {
    "user": "eolowynayhvayxbhluzaqxfp@psql-mock-database-cloud",
    "password": "hdzvzutlssuozdonhflhwyjm",
    "driver": "org.postgresql.Driver"
}

# Carregar as tabelas em DataFrames
dataframes = {}
table_names = ["customers", "employees", "offices", "orderdetails", "orders", "payments", "product_lines", "products", "pg_stat_statements"]

for table in table_names:
    dataframes[table] = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
```

## Consultas SQL Realizadas

### 1. Análise do País com a Maior Quantidade de Itens Cancelados

```python
spark.sql("""
SELECT o.country, COUNT(*) AS canceled_items
FROM orders o
WHERE o.status = 'Canceled'
GROUP BY o.country
ORDER BY canceled_items DESC
LIMIT 1
""").show()
```
### Mostrar o resultado da consulta
canceled_items_df.show()

### Salvar o resultado em uma tabela Delta
canceled_items_df.write.format("delta").mode("overwrite").save("/mnt/delta/canceled_items")

### 2. Faturamento da Linha de Produto Mais Vendida em 2005

```python
spark.sql("""
SELECT pl.product_line, SUM(od.quantity_ordered * od.price_each) AS total_revenue
FROM orders o
JOIN orderdetails od ON o.order_number = od.order_number
JOIN products p ON od.product_code = p.product_code
JOIN product_lines pl ON p.product_line = pl.product_line
WHERE o.status = 'Shipped'
AND YEAR(o.order_date) = 2005
GROUP BY pl.product_line
ORDER BY total_revenue DESC
LIMIT 1
""").show()
```

### Mostrar o resultado da consulta
revenue_df.show()

### Salvar o resultado em uma tabela Delta
revenue_df.write.format("delta").mode("overwrite").save("/mnt/delta/revenue_2005")

### 3. Nome, Sobrenome e E-mail Mascarado dos Vendedores no Japão

```python
spark.sql("""
SELECT e.first_name, e.last_name, 
       CONCAT('***', SUBSTRING(e.email, POSITION('@' IN e.email), LENGTH(e.email))) AS masked_email
FROM employees e
JOIN offices o ON e.office_code = o.office_code
WHERE o.country = 'Japan'
""").show()
```
### Mostrar o resultado da consulta
masked_email_df.show()

### Salvar o resultado em uma tabela Delta
masked_email_df.write.format("delta").mode("overwrite").save("/mnt/delta/masked_emails_japan")

## Conclusão

Este projeto demonstrou como utilizar o Databricks para conectar a um banco de dados PostgreSQL e realizar análises de dados utilizando SQL. As consultas foram projetadas para responder a perguntas específicas relacionadas a vendas e clientes, fornecendo insights valiosos.

## Pré-requisitos

- Acesso ao Databricks.
- Credenciais para o banco de dados PostgreSQL.
- Bibliotecas necessárias instaladas no cluster (ex.: `org.postgresql.Driver`).


