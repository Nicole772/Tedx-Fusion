// handler.js

const { ComprehendClient, DetectSentimentCommand } = require("@aws-sdk/client-comprehend");
const connect_to_db = require('./db');
const Talk = require('./Next');

// Istanzia il client Comprehend
const comprehendClient = new ComprehendClient({ region: "us-east-1" });

module.exports.get_highlight_by_talk = (event, context, callback) => {
  // Impedisce che la funzione Lambda attenda che l'event loop sia vuoto prima di terminare
  context.callbackWaitsForEmptyEventLoop = false;

  console.log('Received event:', JSON.stringify(event, null, 2));

  // Analizza il body della richiesta
  let body = {};
  if (event.body) {
    try {
      body = JSON.parse(event.body);
    } catch (error) {
      callback(null, {
        statusCode: 400,
        headers: { 'Content-Type': 'text/plain' },
        body: 'Invalid JSON in request body.'
      });
      return;
    }
  }

  // Verifica che l'ID del talk sia presente nel body della richiesta
  if (!body._id) {
    callback(null, {
      statusCode: 500,
      headers: { 'Content-Type': 'text/plain' },
      body: 'Could not fetch highlights. Video ID is null.'
    });
    return;
  }

  // Connette al database e poi esegue la query
  connect_to_db()
    .then(() => {
      console.log('=> Getting highlights for talk ID:', body._id);

      // Trova il talk corrispondente all'ID
      Talk.findOne({ _id: body._id })
        .then(talk => {
          // Verifica se è stato trovato un talk e se ha una trascrizione
          if (!talk || !talk.transcription) {
            callback(null, {
              statusCode: 404,
              body: JSON.stringify({ error: "Video not found or no transcription available" })
            });
            return;
          }

          // Suddividi la trascrizione in frasi
          const sentences = talk.transcription.match(/[^.!?]+[.!?]/g) || [];

          // Array per memorizzare i segmenti evidenziati
          let highlightedSegments = [];

          // Promesse per l'analisi del sentiment
          const sentimentPromises = sentences.map(async (sentence, index) => {
            const params = {
              Text: sentence,
              LanguageCode: "en"
            };

            try {
              // Crea un comando per l'analisi del sentiment
              const command = new DetectSentimentCommand(params);
              // Esegui il comando
              const sentimentData = await comprehendClient.send(command);

              const sentiment = sentimentData.Sentiment;

              // Genera timestamp fittizio (migliorabile con dati reali)
              const timestamp = generateTimestamp(index);

              // Aggiungi ai risultati solo le frasi più rilevanti
              if (sentiment !== "NEUTRAL") {
                highlightedSegments.push({
                  text: sentence.trim(),
                  timestamp: timestamp,
                  sentiment: sentiment
                });
              }
            } catch (error) {
              console.error("Error analyzing sentiment:", error);
              // Continua con la prossima frase
            }
          });

          // Attendi che tutte le analisi del sentiment siano completate
          Promise.all(sentimentPromises)
            .then(() => {
              // Costruisci la risposta JSON
              const response = {
                video_id: body._id,
                sentiment: analyzeOverallSentiment(highlightedSegments),
                highlights: highlightedSegments
              };

              callback(null, {
                statusCode: 200,
                body: JSON.stringify(response)
              });
            })
            .catch(err => {
              console.error('Error during sentiment analysis:', err);
              callback(null, {
                statusCode: 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Failed to analyze text sentiment.'
              });
            });
        })
        .catch(err => {
          console.error('Error during query:', err);
          callback(null, {
            statusCode: err.statusCode || 500,
            headers: { 'Content-Type': 'text/plain' },
            body: 'Could not fetch the talk transcript.'
          });
        });
    })
    .catch(err => {
      console.error('Database connection failed:', err);
      callback(null, {
        statusCode: 500,
        headers: { 'Content-Type': 'text/plain' },
        body: 'Database connection failed.'
      });
    });
};

// Funzione per generare timestamp fittizi
function generateTimestamp(index) {
  const minutes = Math.floor(index / 2);
  const seconds = (index % 2) * 30;
  return `${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
}

// Funzione per determinare il sentiment globale
function analyzeOverallSentiment(highlights) {
  if (highlights.length === 0) return "NEUTRAL";

  const sentiments = highlights.map(h => h.sentiment);
  const sentimentCount = sentiments.reduce((acc, s) => {
    acc[s] = (acc[s] || 0) + 1;
    return acc;
  }, {});

  return Object.keys(sentimentCount).reduce((a, b) =>
    sentimentCount[a] > sentimentCount[b] ? a : b, "NEUTRAL");
}
