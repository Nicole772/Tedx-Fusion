const mongoose = require("mongoose");

const talkSchema = new mongoose.Schema({
    _id: String,  // ID univoco del TEDx Talk
    title: String,  // Titolo del talk
    transcription: String,  // Testo della trascrizione
    watch_next: [  // Lista di video consigliati
        {
            title: String,
            url: String,
            speakers: String,
            publishedAt: String
        }
    ]
});

// Esporta modello per poterlo usare nell'handler
module.exports = mongoose.model("Talk", talkSchema);
