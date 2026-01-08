// Switch to FlexiMart NoSQL database
db = connect("mongodb://localhost:27017/fleximart_nosql")


// ==================================================
// Operation 1: Load Data
// MongoDB Operations for FlexiMart Project

// 1. View all products
db.products.find({})

// 2. Find products with price greater than 1000
db.products.find({ price: { $gt: 1000 } })

// 3. Count total products
db.products.countDocuments()

// 4. Aggregate: average price by category
db.products.aggregate([
  {
    $group: {
      _id: "$category",
      avgPrice: { $avg: "$price" }
    }
  }
])

// 5. Find products with low stock
db.products.find({ stock: { $lt: 20 } })
// ==================================================


// ==================================================
// Operation 2: Basic Query
// Find all products in "Electronics" category
// with price less than 50000
// Return only: name, price, stock
// ==================================================

db.products.find(
  {
    category: "Electronics",
    price: { $lt: 50000 }
  },
  {
    _id: 0,
    name: 1,
    price: 1,
    stock: 1
  }
);


// ==================================================
// Operation 3: Review Analysis
// Find all products that have average rating >= 4.0
// Use aggregation to calculate average rating
// ==================================================

db.products.aggregate([
  { $unwind: "$reviews" },
  {
    $group: {
      _id: "$name",
      avg_rating: { $avg: "$reviews.rating" }
    }
  },
  {
    $match: {
      avg_rating: { $gte: 4.0 }
    }
  }
]);


// ==================================================
// Operation 4: Update Operation
// Add a new review to product "ELEC001"
// ==================================================

db.products.updateOne(
  { product_id: "ELEC001" },
  {
    $push: {
      reviews: {
        user_id: "U999",
        username: "ValueBuyer",
        rating: 4,
        comment: "Good value for money",
        date: new Date()
      }
    }
  }
);


// ==================================================
// Operation 5: Complex Aggregation
// Calculate average price by category
// Return: category, avg_price, product_count
// Sort by avg_price descending
// ==================================================

db.products.aggregate([
  {
    $group: {
      _id: "$category",
      avg_price: { $avg: "$price" },
      product_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      category: "$_id",
      avg_price: 1,
      product_count: 1
    }
  },
  {
    $sort: { avg_price: -1 }
  }
]);
