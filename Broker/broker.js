// broker.js
const zmq = require('zeromq')


const brokerIp = `127.0.0.1`;
const BROKER_PUB_PORT = 3000;
const BROKER_SUB_PORT = 3001;

const BROKER_REP_PORT = 3002;

//CLIENTE -> COORDINADOR
const COD_PUB = 1; // Cliente publica un nuevo mensaje

const COD_ALTA_SUB = 2; // Cliente se suscribe a un tópico


//COORDINADOR -> BROKER
const COD_ADD_TOPICO_BROKER = 3;// Coordinador informa al broker un nuevo topico 


//SERVIDOR HTTP -> BROKER
const COD_GET_TOPICOS = 4; // Servidor solicita a todos los sus topicos 

const COD_BORRAR_MENSAJES = 6; // Servidor solicita a un broker que borre sus mensajes

//SERVIDOR HTTP -> BROKER
const COD_GET_MENSAJES_COLA = 5;// Servidor solicita a todos los brokers todos sus mensajes

let listaTopicos = [];



// TODO LIST:
// - Agregar la cola de envio de mensajes de cada tópico que se maneja
// - Gestionar la listaTopicos con las adiciones
// - Validar los mensajes entrantes, que cumplan con las condiciones de la cola de mensajes, y que sea un tópico válido del broker. else droppearlos
// - Hacer la subrutina que borre los mensajes que no cumplan con el tiempo que tienen que cumplir
// - Hacer la subrutina que borre los mensajes que no cumplan con la ocupación que tienen que cumplir



// definimos 2 sockets: el que escucha todos los mensajes entrantes (subSocket), y el que va a enviar los mensajes a destino (pubSocket)
const subSocket = zmq.socket('xsub'),  // el broker escucha a todos los publisher
		pubSocket = zmq.socket('xpub') // el broker le manda a todos los subscriber

const repSocket = zmq.socket('req');

// ambos sockets van a ser modo servidor, ya que los publicadores y suscriptores de nuestro sistema, los clientes, se conectaran al broker (y no al revés)
subSocket.bindSync(`tcp://${brokerIp}:${BROKER_SUB_PORT}`) //si yo quiero publicar me comunico con este
pubSocket.bindSync(`tcp://${brokerIp}:${BROKER_PUB_PORT}`) //si yo quiero escuchar me suscribo a este

repSocket.on('message',cbRep);

function cbRep(replyJSON){
	let reply = JSON.parse(replyJSON);
	switch (reply.accion) {
		//COORDINADOR
		case COD_ADD_TOPICO_BROKER: 
			//TODO al enviar solo el codigo, el coordinador no sabe que broker le envio
			let mensaje = {
				code : 0
			}
			repSocket.send(JSON.stringify(mensaje));
			break;
		
		//SERVIDOR HTTP
		case COD_GET_TOPICOS:
		
			break;

		case COD_GET_MENSAJES_COLA:
			
				break;

		case COD_BORRAR_MENSAJES:

			break;			
	
		default:
			break;
	}
}

// redirige todos los mensajes que recibimos
subSocket.on('message', function (topic, message) {

	//fijarse si el topico es valido

	pubSocket.send([topic, message])
  	console.log(` REDIRIGIENDO MSJ topico: ${topic}, mensaje: ${message}`);
})



// cuando el pubSocket recibe un tópico, subSocket debe subscribirse a él; para eso se utiliza el método send
// le manda el topico al que se subscribe (es una especie de confirmacion)
pubSocket.on('message', function (topic) {
	if(listaTopicos.includes(topic)){
		subSocket.send(topic);
		console.log(`topic: ${topic}`);
	} else {
		console.log(`invalid topic: ${topic}`);
	}
	

	
})