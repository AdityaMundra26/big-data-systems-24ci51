// ==============================================================================
// MONGODB SHELL (MONGOSH) SCRIPTS: E-COMMERCE PRODUCT CATALOG
// ==============================================================================

use ecommerce_catalog_db;

// 1. CREATE COLLECTION WITH JSON SCHEMA VALIDATION
db.createCollection("products", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["sku", "title", "brand", "category", "price", "inventory"],
      properties: {
        sku: { bsonType: "string", description: "Must be a string and is required" },
        title: { bsonType: "string", description: "Product title string" },
        brand: { bsonType: "string" },
        category: { bsonType: "string" },
        price: { bsonType: ["double", "int", "decimal"], minimum: 0 },
        inventory: {
          bsonType: "object",
          required: ["stock_quantity"],
          properties: {
            stock_quantity: { bsonType: "int", minimum: 0 },
            warehouse_location: { bsonType: "string" }
          }
        },
        attributes: { bsonType: "object", description: "Flexible polymorphic specs" }
      }
    }
  }
});

// 2. CREATE INDEXES FOR FAST RETRIEVAL & SEARCH
// Single-field index on category
db.products.createIndex({ category: 1 }, { name: "idx_category" });

// Compound index for category filtering and price sorting/range queries
db.products.createIndex({ category: 1, price: -1 }, { name: "idx_cat_price_desc" });

// Multikey index for array-based product tags
db.products.createIndex({ tags: 1 }, { name: "idx_tags_multikey" });

// Text index for full-text search across titles and brands
db.products.createIndex({ title: "text", brand: "text" }, { name: "idx_text_search" });

// 3. POLYMORPHIC PRODUCT INSERTIONS
db.products.insertMany([
  {
    sku: "DELL-XPS15-2024",
    title: "Dell XPS 15 High Performance Laptop",
    brand: "Dell",
    category: "Electronics",
    price: 1499.99,
    rating: { score: 4.7, review_count: 340 },
    tags: ["laptop", "ultrabook", "developer"],
    attributes: {
      processor: "Intel Core i7-13700H",
      ram_gb: 32,
      storage: "1TB NVMe SSD",
      gpu: "NVIDIA GeForce RTX 4060"
    },
    inventory: { stock_quantity: 45, warehouse_location: "US-EAST-01" },
    lifecycle_status: "ACTIVE"
  },
  {
    sku: "NIKE-AF1-WHT",
    title: "Nike Air Force 1 '07 Sneakers",
    brand: "Nike",
    category: "Fashion",
    price: 115.00,
    rating: { score: 4.8, review_count: 4200 },
    tags: ["shoes", "sneakers", "streetwear"],
    attributes: {
      sizes_available: [8, 9, 10, 11],
      material: "Leather",
      color: "White"
    },
    inventory: { stock_quantity: 150, warehouse_location: "US-CENTRAL-01" },
    lifecycle_status: "ACTIVE"
  }
]);

// 4. RETRIEVAL WITH EXPLAIN (EXECUTION STATS)
db.products.find({
  category: "Electronics",
  price: { $gte: 500, $lte: 2000 }
}).sort({ price: -1 }).explain("executionStats");

// 5. ATOMIC STOCK DECREMENT ON CHECKOUT
db.products.updateOne(
  { sku: "DELL-XPS15-2024", "inventory.stock_quantity": { $gte: 1 } },
  {
    $inc: { "inventory.stock_quantity": -1 },
    $set: { "last_purchase_timestamp": new Date() }
  }
);

// 6. PURGE OBSOLETE / DISCONTINUED ITEMS
db.products.deleteMany({
  lifecycle_status: "DISCONTINUED",
  "inventory.stock_quantity": { $lte: 0 }
});

// 7. AGGREGATION PIPELINE: CATEGORY METRICS
db.products.aggregate([
  { $match: { lifecycle_status: "ACTIVE" } },
  {
    $group: {
      _id: "$category",
      total_products: { $sum: 1 },
      avg_price: { $avg: "$price" },
      min_price: { $min: "$price" },
      max_price: { $max: "$price" },
      total_inventory_units: { $sum: "$inventory.stock_quantity" }
    }
  },
  { $sort: { total_products: -1 } }
]);
