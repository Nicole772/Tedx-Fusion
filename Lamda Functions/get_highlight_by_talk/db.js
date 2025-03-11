const mongoose = require("mongoose");
require("dotenv").config();

let isConnected = false; // Variabile globale per evitare connessioni ripetute

const connectToDatabase = async () => {
    if (isConnected) {
        console.log("🔄 Using existing MongoDB connection");
        return;
    }

    try {
        const db = await mongoose.connect(process.env.MONGO_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });
        isConnected = db.connections[0].readyState;
        console.log("✅ MongoDB connected");
    } catch (error) {
        console.error("❌ MongoDB connection failed:", error);
        throw new Error("Database connection failed");
    }
};

module.exports = connectToDatabase;
