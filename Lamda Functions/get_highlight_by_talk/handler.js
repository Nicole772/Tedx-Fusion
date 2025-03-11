const AWS = require("aws-sdk");
const connectToDatabase = require("./db");
const Talk = require("./Next");

AWS.config.update({ region: "us-east-1" });
const comprehend = new AWS.Comprehend();

// Lambda Handler
exports.getHighlightedSegments = async (event) => {
    try {
        // 1️⃣ Estrai `video_id` dalla richiesta
        const { video_id } = event.queryStringParameters;
        if (!video_id) {
            return {
                statusCode: 400,
                body: JSON.stringify({ error: "Missing video_id" })
            };
        }

        // 2️⃣ Connetti a MongoDB e recupera la trascrizione
        await connectToDatabase();
        const talk = await Talk.findOne({ _id: video_id });

        if (!talk || !talk.transcription) {
            return {
                statusCode: 404,
                body: JSON.stringify({ error: "Video not found or no transcription available" })
            };
        }

        // 3️⃣ Suddividi la trascrizione in frasi e analizza con AWS Comprehend
        const sentences = talk.transcription.match(/[^.!?]+[.!?]/g) || [];
        let highlightedSegments = [];

        for (const sentence of sentences) {
            const params = {
                Text: sentence,
                LanguageCode: "en"
            };

            // Analisi del sentiment
            const sentimentData = await comprehend.detectSentiment(params).promise();
            const sentiment = sentimentData.Sentiment;

            // Genera timestamp fittizio (migliorabile con dati reali)
            const timestamp = generateTimestamp(sentences.indexOf(sentence));

            // Aggiungi ai risultati solo le frasi più rilevanti
            if (sentiment !== "NEUTRAL") {
                highlightedSegments.push({
                    text: sentence.trim(),
                    timestamp: timestamp,
                    sentiment: sentiment
                });
            }
        }

        // 4️⃣ Costruisci la risposta JSON
        const response = {
            video_id: video_id,
            sentiment: analyzeOverallSentiment(highlightedSegments),
            highlights: highlightedSegments
        };

        return {
            statusCode: 200,
            body: JSON.stringify(response)
        };

    } catch (error) {
        console.error("Error:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: "Internal server error" })
        };
    }
};

// Funzione per generare timestamp fittizi
function generateTimestamp(index) {
    const minutes = Math.floor(index / 2);
    const seconds = (index % 2) * 30;
    return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

// Funzione per determinare il sentiment globale
function analyzeOverallSentiment(highlights) {
    const sentiments = highlights.map(h => h.sentiment);
    const sentimentCount = sentiments.reduce((acc, s) => {
        acc[s] = (acc[s] || 0) + 1;
        return acc;
    }, {});

    return Object.keys(sentimentCount).reduce((a, b) => sentimentCount[a] > sentimentCount[b] ? a : b, "NEUTRAL");
}
