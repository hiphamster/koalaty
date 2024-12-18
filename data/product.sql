CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    units ENUM('lbs', 'kg', 'oz', 'g', 'each', '%') DEFAULT 'oz' NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    average_weight DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
