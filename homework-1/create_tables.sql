-- SQL-команды для создания таблиц

CREATE TABLE employees
(
    employee_id integer PRIMARY KEY,
    first_name varchar(50) NOT NULL,
	last_name varchar(50) NOT NULL,
	title varchar(100) NOT NULL,
	birth_date date,
	notes text
);

CREATE TABLE customers
(
    customer_id varchar(100) NOT NULL PRIMARY KEY,
    company_name varchar(100) NOT NULL,
	contact_name varchar(100) NOT NULL
);

CREATE TABLE orders
(
    order_id integer PRIMARY KEY,
	customer_id varchar(100) NOT NULL REFERENCES customers(customer_id),
    employee_id integer REFERENCES employees(employee_id),
	order_date date,
	ship_city varchar(50) NOT NULL
);
