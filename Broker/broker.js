// broker.js
const zmq = require('../zeromq/node_modules/zeromq');

const globals = require('../Global/Globals');

let config = require('./configBroker.json');
let configClientNTP = require('../Global/configClientNTP.json');
const { COD_ERROR_TOPICO_INEXISTENTE } = require('../Global/Globals');
console.log(config);

// TODO - Sacar estos datos de un archivo JSON
const brokerIp = config.ip;
const BROKER_PUB_PORT = config.pubPort;
const BROKER_SUB_PORT = config.subPort;

const BROKER_REP_PORT = config.repPort;

const MAX_MENSAJES_COLA = config.maxMensajesCola;  //15
const MAX_DIF_TIEMPO_MENSAJE = 1000 * config.maxDifTiempoMensaje; // en milisegundos 300000


const INTERVALO_VERIF_EXP_MSJ = 1000 * config.intervaloVerifExpMsj; // cada cuanto tiempo se verifica el tiempo de expiracion de los mensajes en la cola de mensajes

// definimos 2 sockets: el que escucha todos los mensajes entrantes (subSocket), y el que va a enviar los mensajes a destino (pubSocket)
const xsubSocket = zmq.socket('xsub'),  // el broker escucha a todos los publisher
	xpubSocket = zmq.socket('xpub') // el broker le manda a todos los subscriber

const repSocket = zmq.socket('rep'); //para contestar al servidor http y al coordinador

let colaMensajesPorTopico = {}; // key = topico, value = cola de mensajes ordenados por fecha ascendente
// cada cola de mensajes esta ordenada por fecha de envio del mensaje

//SERVER NTP
const net = require('net');

const portNTP = configClientNTP.portNTP;
const NTP_IP = configClientNTP.ipNTP;
const INTERVAL_NTP = 1000 * configClientNTP.intervalNTP; // seconds 1
const INTERVAL_PERIODO = 1000 * configClientNTP.intervalPeriodo;  //seconds 120
const INTERVAL_ENVIO_HEARTBEAT = 1000 * config.intervalEnvioHeartbeat;
const TOLERANCIA_CLIENTE = 1000 * config.toleranciaCliente;
const i = total = configClientNTP.cantOffsetsNTP;
let offsetHora = 0;
let offsetAvg = 0;

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
// - responder solicitudes del server HTTP





// TODO PROM:

// - ?¿? mandar cola de mensajes de un grupo a los que se conectan por primera vez o si hay reconexiones.


// PP

{
	// ambos sockets van a ser modo servidor, ya que los publicadores y suscriptores de nuestro sistema, los clientes, se conectaran al broker (y no al revés)
	
	initClientNTP();
	initSubSocket();
	initPubSocket();
	
	initRepSocket();
	let mensaje1 = {
		mensaje: 'hola',
		fecha: '2020-11-27T19:13:20',
		emisor: 'fer'
	}
	colaMensajesPorTopico = {topico: [mensaje1]}

	validarTiempoExpiracionMensajes();
}


function validarTiempoExpiracionMensajes() {
	setInterval(function () {
		//se usa la key, para poder conocer la posicion de la cola en la lista
		//sino hay un tema de referencias feas

		// tomamos la cola de mensajes de cada topico
		Object.keys(colaMensajesPorTopico).forEach(key => {
			console.log(colaMensajesPorTopico[key]);
			colaMensajesPorTopico[key] = colaMensajesPorTopico[key].filter(mensaje => 
				(new Date(getTimeNTP()).getTime() - new Date(mensaje.fecha).getTime() <= MAX_DIF_TIEMPO_MENSAJE)
				// solo dejamos mensajes que tengan menor diferencia de fecha con la actual a la permitida
			
			);
			console.log(new Date(getTimeNTP()).getTime());
			console.log(colaMensajesPorTopico[key]);
		});
	}, INTERVALO_VERIF_EXP_MSJ);
}


// inicializa el socket para recibir publicaciones y mandarlas a clientes a traves del subSocket
function initPubSocket() {
	// se conectan los suscriptores esperando recibir mensajes
	xpubSocket.on('message', function (topic) {
		let topicSinHeader = topic.toString().substring(1);
		if (colaMensajesPorTopico.hasOwnProperty(topicSinHeader)) { // el topico es valido QAOP
			xsubSocket.send(topic); // el broker se suscribe a ese topico para poder recibir mensajes de sus publicadores 
			console.log(`Se conecto un suscriptor a topico: ${topicSinHeader}`);

		} else { // le pidieron publicar en un topico que no administra
			console.log(`Error 1239123: Suscripcion a un topico invalido: ${topicSinHeader}`);
		}

	})

	xpubSocket.bindSync(`tcp://${brokerIp}:${BROKER_PUB_PORT}`) //si un cliente quiere escuchar se suscribe a este
}

// inicializa el socket para mandar lo que se publica a los clientes
function initSubSocket() {
	// se conectan los publicadores para que su mensaje sea redirigido a los suscriptores
	xsubSocket.on('message', function (topic, message) {
		
		if (colaMensajesPorTopico.hasOwnProperty(topic)) { // el topico es valido 
			console.log(` LLEGO MSJ -> - topico: ${topic}, mensaje: ${message}`);
			procesaMensaje(topic, message);
		}
		else {
			console.log(` DROPEANDO MSJ -> no hay topico valido - topico: ${topic}, mensaje: ${message}`);
        }
	});

	xsubSocket.bindSync(`tcp://${brokerIp}:${BROKER_SUB_PORT}`) //si un cliente quiere publicar se comunica con este
}

// si la cola de mensajes tiene espacio -> inserta el mensaje en la cola
// si la cola de mensajes no tiene espacio y el mensaje que llego es mas reciente que el mas viejo de la cola -> descarta el mensajes mas antiguo para que haya espacio
function procesaMensaje(topico, mensajeJSON) {
	let colaMensajes = colaMensajesPorTopico[topico];
	let mensaje = JSON.parse(mensajeJSON);
	if (colaMensajes.length === 0 || mensaje.fecha > colaMensajes[0].fecha) { // el mensaje cumple la condicion de la cola
		
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
	xpubSocket.send([topico, mensaje]) // distribucion del mensaje a suscriptores
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
	let mensaje;

	switch (request.accion) {
		//COORDINADOR
		case globals.COD_ADD_TOPICO_BROKER:
			if (!colaMensajesPorTopico.hasOwnProperty(topico)) { // le piden administrar a un topico que no administraba
				agregarColaMensajes(topico);
            }

			mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, {});
			// los resultados van vacíos porque el coord ya sabe ip y puertos del broker

			repSocket.send(JSON.stringify(mensaje));

			console.log(`Asignaron el topico ${requestJSON}`);
			break;

		//SERVIDOR HTTP
		case globals.COD_GET_TOPICOS:
			let topicos = globals.getKeys(colaMensajesPorTopico);

			let resultados = {
				listaTopicos:topicos // ['topico1','topico2',...]
			};

			mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, resultados);
			// los resultados van vacíos porque el coord ya sabe ip y puertos del broker

			repSocket.send(JSON.stringify(mensaje));

			console.log(`Solicitaron los topicos`);
			break;

		case globals.COD_GET_MENSAJES_COLA:
			
			if (colaMensajesPorTopico.hasOwnProperty(topico)) { 
				//manda cola de mensajes de ese topico
				let resultados = {
					mensajes: colaMensajesPorTopico[topico] // ['mensaje1','mensaje2',...]
				};
				mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, resultados);
			}
			else{
				//no maneja ese topico -> manda rta con codigo de error
				mensaje = globals.generarRespuestaNoExitosa(request.accion, request.idPeticion, globals.COD_ERROR_TOPICO_INEXISTENTE, `El topico ${topico} no es administrado por este broker`);
			}

			repSocket.send(JSON.stringify(mensaje));

			console.log(`Solicitaron los mensajes del topico ${topico}`);
			break;

		case globals.COD_BORRAR_MENSAJES:
			if (colaMensajesPorTopico.hasOwnProperty(topico)) { 
				//borra los mensajes de ese topico porque lo administraba
				borrarColaMensajesTopico(topico);
				let resultados = {};
				mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, resultados);
			}
			else{
				//no maneja ese topico -> no borra mensajes
				mensaje = globals.generarRespuestaNoExitosa(request.accion, request.idPeticion, globals.COD_ERROR_TOPICO_INEXISTENTE, `El topico ${topico} no es administrado por este broker`);
			
			}

			console.log(`Solicitaron borrar la cola de mensajes del topico ${topico}`);
			break;

		default:
			mensaje = globals.generarRespuestaNoExitosa(request.accion, request.idPeticion, globals.COD_ERROR_OPERACION_INEXISTENTE, 'El codigo de operacion ingresado no se reconoce como un codigo valido');
			repSocket.send(JSON.stringify(mensaje));

			console.log(`Error ${request.error.codigo}: ${request.error.mensaje}`);
			break;
	}
	
}

// dado un topico crea su cola de mensajes
function agregarColaMensajes(topico) {
	colaMensajesPorTopico[topico] = [];
}

// dado un topico borra el contenido de su cola de mensajes
function borrarColaMensajesTopico(topico){
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

//////////////////////////////////////////////////////////////////////////////////////
//                                CLIENT (BROKER) NTP                               //                                        
//////////////////////////////////////////////////////////////////////////////////////

function getTimeNTP(){
    var dateObj = Date.now();

    // Add 3 days to the current date & time
    //   I'd suggest using the calculated static value instead of doing inline math
    //   I did it this way to simply show where the number came from
    dateObj += offsetHora;

	// create a new Date object, using the adjusted time
	//console.log(Date(dateObj).toISOString());
    return new Date(dateObj).toISOString();    
}

function enviarTiemposNTP(){
    let idIntervalo = setInterval(function () {
      if (i--) {
      //Calculo cada offset, en un intervalo determinado
          let T1 = (new Date()).getTime();
          let mensaje = {
              t1: T1.toISOString(), 
          }
          client.write(JSON.stringify(mensaje));
      } 
      else {
      //Luego de calcular los N offsets (fin del intervalo), asigno el offset del cliente
          clearInterval(idIntervalo);
          console.log('Delay promedio: ' + offsetAvg + 'ms');
          offsetHora = offsetAvg;
      }
    }, INTERVAL_NTP);
  }

function initClientNTP(){
	let client = net.createConnection(portNTP, NTP_IP, sincronizacionNTP);
    client.on('data', function (data) {
        
        let T4 = new Date(new Date().toISOString()).getTime();
    
        // obtenemos hora del servidor
        let T1 = new Date(data.t1).getTime();
        let T2 = new Date(data.t2).getTime();
        let T3 = new Date(data.t3).getTime();
    
        // calculamos delay de la red
        // var delay = ((T2 - T1) + (T4 - T3)) / 2;
        let offsetDelNTP = ((T2 - T1) + (T3 - T4)) / 2;
        offsetAvg += offsetDelNTP/total;
        
    
        console.log('offset red:\t\t' + offsetDelNTP + ' ms');
        console.log('---------------------------------------------------');
    });
    
}


function sincronizacionNTP(){
    //Espera 2 minutos antes de enviar una nueva peticion al servidor NTP
    setInterval(function () {
        enviarTiemposNTP();
    }, INTERVAL_PERIODO);      
}


//////////////////////////////////////////////////////////////////////////////////////
//                                      NTP                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////



