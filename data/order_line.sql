CREATE TABLE IF NOT EXISTS order_lines (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    /* calculate total on_update */
    total DECIMAL(10, 2) NULL,
    extra JSON,
    /* FOREIGN KEY (customer_id) REFERENCES customer(customer_id), */
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id),
    CONSTRAINT unique_order_id__product_id UNIQUE (order_id, product_id)
);
