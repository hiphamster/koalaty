CREATE TABLE if NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    close_date TIMESTAMP NULL,
    /* stage_name ENUM('open', 'pickup_ready', 'closed_received', 'closed_lost') DEFAULT 'open' NOT NULL, */
    stage_name VARCHAR(50) DEFAULT 'Order placed',
    extra JSON,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    CONSTRAINT unique_customer_date UNIQUE (customer_id, order_date)
);
