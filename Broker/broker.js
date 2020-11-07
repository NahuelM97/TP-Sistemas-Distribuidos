// broker.js
const zmq = require('zeromq')


const brokerIp = `127.0.0.1`;
const brokerPubPort = 3000;
const brokerSubPort = 3001;


// definimos 2 sockets: el que escucha todos los mensajes entrantes (subSocket), y el que va a enviar los mensajes a destino (pubSocket)
const subSocket = zmq.socket('xsub'),  // el broker escucha a todos los publisher
			pubSocket = zmq.socket('xpub') // el broker le manda a todos los subscriber

// ambos sockets van a ser modo servidor, ya que los publicadores y suscriptores de nuestro sistema, los clientes, se conectaran al broker (y no al revés)
subSocket.bindSync(`tcp://${brokerIp}:${brokerSubPort}`) //si yo quiero publicar me comunico con este
pubSocket.bindSync(`tcp://${brokerIp}:${brokerPubPort}`) //si yo quiero escuchar me suscribo a este

// redirige todos los mensajes que recibimos
subSocket.on('message', function (topic, message) {
  pubSocket.send([topic, message])
  console.log(` REDIRIGIENDO MSJ topico: ${topic}, mensaje: ${message}`);
})



// cuando el pubSocket recibe un tópico, subSocket debe subscribirse a él; para eso se utiliza el método send
// le manda el topico al que se subscribe (es una especie de confirmacion)
pubSocket.on('message', function (topic) {
	subSocket.send(topic)

	console.log(`topic: ${topic}`)
})