// broker.js
const zmq = require('../zeromq/node_modules/zeromq');

const globals = require('../Global/Globals');

// TODO - Sacar estos datos de un archivo JSON
const brokerIp = `127.0.0.1`;
const BROKER_PUB_PORT = 3000;
const BROKER_SUB_PORT = 3001;

const BROKER_REP_PORT = 3002;

const MAX_MENSAJES_COLA = 15;
const MAX_DIF_TIEMPO_MENSAJE = 300000; // en milisegundos


const INTERVALO_VERIF_EXP_MSJ = 20; // cada cuanto tiempo se verifica el tiempo de expiracion de los mensajes en la cola de mensajes

// definimos 2 sockets: el que escucha todos los mensajes entrantes (subSocket), y el que va a enviar los mensajes a destino (pubSocket)
const subSocket = zmq.socket('xsub'),  // el broker escucha a todos los publisher
	pubSocket = zmq.socket('xpub') // el broker le manda a todos los subscriber

const repSocket = zmq.socket('req');

let colaMensajesPorTopico = {}; // key = topico, value = cola de mensajes ordenados por fecha ascendente
// cada cola de mensajes esta ordenada por fecha de envio del mensaje




// TODO LIST:
// -x Agregar la cola de envio de mensajes de cada tópico que se maneja
// -x Gestionar la listaTopicos con las adiciones
// -x Validar los mensajes entrantes, que cumplan con las condiciones de la cola de mensajes, y que sea un tópico válido del broker. else droppearlos
// -x Hacer la subrutina que borre los mensajes que no cumplan con el tiempo que tienen que cumplir
// -x Hacer la subrutina que borre los mensajes que no cumplan con la ocupación que tienen que cumplir
// -x Establecer tiempo Y y cant de mensajes X para las colas de mensajes
// - usar protocolo REQREP entre broker y coord para pedir ip+puerto del broker que maneje el topico heartbeat para poder suscribirse
// - usar protocolo REQREP entre broker y coord para pedir ip+puerto de un broker que maneje un topico para la publicacion en los topicos message/<user>
//   que se necesiten para mandar colas de mensajes
// - mantener el estado de conexion de los clientes escuchando el topico heartbeat
// - cuando un cliente se suscribe por primera vez a un topico o se reconecta, hay que mandarle las colas de mensajes de message/<user> y el message/all a
//   su message/<user> con el protocolo PUBSUB
// - sincronizar reloj con NTP





// TODO PROM:

// - ?¿? mandar cola de mensajes de un grupo a los que se conectan por primera vez o si hay reconexiones.


// PP

{
	// ambos sockets van a ser modo servidor, ya que los publicadores y suscriptores de nuestro sistema, los clientes, se conectaran al broker (y no al revés)
	
	
	initSubSocket();
	initPubSocket();
	
	initRepSocket();

	validarTiempoExpiracionMensajes();
}


function validarTiempoExpiracionMensajes() {
	setInterval(function () {
		// tomamos la cola de mensajes de cada topico
		Object.values(colaMensajesPorTopico).forEach(colaMensajes => {

			colaMensajes = colaMensajes.filter(mensaje => {
				(new Date().getTime() - new Date(colaMensajes[i].fecha).getTime() <= MAX_DIF_TIEMPO_MENSAJE);
				// solo dejamos mensajes que tengan menor diferencia de fecha con la actual a la permitida
			}
			);

		});
	}, INTERVALO_VERIF_EXP_MSJ);
}


// inicializa el socket para recibir publicaciones y mandarlas a clientes a traves del subSocket
function initPubSocket() {
	// se conectan los suscriptores esperando recibir mensajes
	pubSocket.on('message', function (topic) {
		if (colaMensajesPorTopico.hasOwnProperty(topic)) { // el topico es valido 
			subSocket.send(topic); // el broker se suscribe a ese topico para poder recibir mensajes de sus publicadores 
			console.log(`topic: ${topic}`);

		} else { // le pidieron publicar en un topico que no administra
			console.log(`invalid topic: ${topic}`);
		}

	})

	pubSocket.bindSync(`tcp://${brokerIp}:${BROKER_PUB_PORT}`) //si yo quiero escuchar me suscribo a este
}

// inicializa el socket para mandar lo que se publica a los clientes
function initSubSocket() {
	// se conectan los publicadores para que su mensaje sea redirigido a los suscriptores
	subSocket.on('message', function (topic, message) {
		if (colaMensajesPorTopico.hasOwnProperty(topic)) { // el topico es valido 
			procesaMensaje(topic, message);
		}
		else {
			console.log(` DROPEANDO MSJ -> no hay topico valido - topico: ${topic}, mensaje: ${message}`);
        }
	});

	subSocket.bindSync(`tcp://${brokerIp}:${BROKER_SUB_PORT}`) //si yo quiero publicar me comunico con este
}

// si la cola de mensajes tiene espacio -> inserta el mensaje en la cola
// si la cola de mensajes no tiene espacio y el mensaje que llego es mas reciente que el mas viejo de la cola -> descarta el mensajes mas antiguo para que haya espacio
function procesaMensaje(topico, mensaje) {
	let colaMensajes = colaMensajesPorTopico[topico];
	
	if (colaMensajes.isEmpty() || mensaje.fecha > colaMensajes[0].fecha) { // el mensaje cumple la condicion de la cola
		
		colaMensajes.push(mensaje);
		colaMensajes.sort(compararMensajesPorFecha); // ordenar por fecha

		if (colaMensajes.length > MAX_MENSAJES_COLA) { 
			colaMensajes.pop(0); // saca al mensaje mas antiguo
		}

		colaMensajesPorTopico[topico] = colaMensajes; // actualizamos la cola de mensajes de ese topico

		publicarMensajeValido(topico, mensaje);


	}
	// else -> no era un mensaje valido segun las condiciones de la cola de mensajes

}




// el mensaje cumple con las condiciones de la cola
function publicarMensajeValido(topico, mensaje) {
	pubSocket.send([topico, mensaje]) // distribucion del mensaje a suscriptores
	console.log(` REDIRIGIENDO MSJ topico: ${topico}, mensaje: ${mensaje}`);
}


// inicializa el socket para responder peticiones del coordinador o el servidor HTTP
function initRepSocket() {
	repSocket.on('message', cbRep);
	repSocket.bind(`tcp://${brokerIp}:${BROKER_REP_PORT}`);
}


// se ejecuta cuando me llega una request del coordinador o el server HTTP
function cbRep(requestJSON){
	let request = JSON.parse(requestJSON);

	let topico = request.topico;
	
	switch (request.accion) {
		//COORDINADOR
		case globals.COD_ADD_TOPICO_BROKER:
			if (!colaMensajesPorTopico.hasOwnProperty(topico)) { // le piden administrar a un topico que no administraba
				agregarColaMensajes(topico);
            }

			let mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, {});
			// los resultados van vacíos porque el coord ya sabe ip y puertos del broker

			repSocket.send(JSON.stringify(mensaje));
			break;

		//SERVIDOR HTTP
		case globals.COD_GET_TOPICOS:

			break;

		case globals.COD_GET_MENSAJES_COLA:

			break;

		case globals.COD_BORRAR_MENSAJES:

			break;

		default:
			globals.generarRespuestaNoExitosa(request.accion, request.idPeticion, globals.COD_ERROR_OPERACION_INEXISTENTE, 'El codigo de operacion ingresado no se reconoce como un codigo valido');
			console.log(`Error ${request.error.codigo}: ${request.error.mensaje}`);
			break;
	}
	
}

// agrega una nueva cola de mensajes para el nuevo topico que va a administrar
function agregarColaMensajes(topico) {
	colaMensajesPorTopico[topico] = [];
}



function compararMensajesPorFecha(mensaje1,mensaje2){
	if (mensaje1.fecha < mensaje2.fecha) {
		return -1;
	}
	if (mensaje1.fecha > mensaje2.fecha) {
		return 1;
	}
	// a debe ser igual b
	return 0;
}





