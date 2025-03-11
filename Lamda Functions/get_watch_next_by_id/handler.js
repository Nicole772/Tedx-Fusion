// Importa la funzione per connettersi al database
const connect_to_db = require('./db'); 

// Importa il modello definito in Next.js (il modello 'talk')
const Talk = require('./Next'); 

// Funzione handler per ottenere i watch next in base all'ID del talk
module.exports.watch_next_by_id = (event, context, callback) => {
  // Impedisce che la funzione Lambda attenda che l'event loop sia vuoto prima di terminare
  context.callbackWaitsForEmptyEventLoop = false;
  
  console.log('Received event:', JSON.stringify(event, null, 2));
  
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
      body: 'Could not fetch the related talks. ID is null.'
    });
    return;
  }
  
  // Imposta valori di default per la paginazione se non specificati
  if (!body.doc_per_page) {
    body.doc_per_page = 10;
  }
  if (!body.page) {
    body.page = 1;
  }


let projection = {};
if(body.slice)
{
    projection = {watch_next : {$slice: body.slice}};
}

connect_to_db().then(() => {
    console.log('=> get_all talks by ID');
    
    Talk.find({ _id: body._id}, projection)
        .skip((body.doc_per_page * body.page) - body.doc_per_page)
        .limit(body.doc_per_page)
        .then(talks => {
            const response = talks.length > 0 ? talks[0].watch_next : [];
            callback(null, {
                statusCode: 200,
                body: JSON.stringify(response)
            });
        })
        .catch(err => {
            console.error('Error fetching talks:', err);
            callback(null, {
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not fetch related talks.'
            });
        });
}).catch(err => {
    console.error('Database connection failed:', err);
    callback(null, {
        statusCode: 500,
        headers: { 'Content-Type': 'text/plain' },
        body: 'Database connection failed.'
    });
});
};