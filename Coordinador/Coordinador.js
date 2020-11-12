var zmq = require('zeromq');

// socket to talk to clients
var repSocket = zmq.socket('rep');
var reqSocket = zmq.socket('req');


const ipCoordinador = `127.0.0.1`;
const puertoCoordinador = 1234;

//CLIENTE -> COORDINADOR
const COD_PUB = 1; // Cliente publica un nuevo mensaje

const COD_ALTA_SUB = 2; // Cliente se suscribe a un tópico


//COORDINADOR -> BROKER
const COD_ADD_TOPICO_BROKER = 3;// Coordinador informa al broker un nuevo topico 


//SERVIDOR HTTP -> BROKER
const COD_GET_TOPICOS = 4; // Servidor solicita a todos los sus topicos 

const COD_BORRAR_MENSAJES = 6; // Servidor solicita a un broker que borre sus mensajes

//SERVIDOR HTTP -> TODOS LOS BROKERS
const COD_GET_MENSAJES_COLA = 5;// Servidor solicita a todos los brokers todos sus mensajes


//key: el topico
//value: la id del broker q lo tiene
//por ej: 'topico1':1
//el 1 es la posicion en el arreglo de brokers
let topicoIdBroker = {};

//key la id del broker
// value el ippuerto
let brokerIpPuerto = [
    { ip: '127.0.0.1', puertoPub: 3000, puertoSub: 3001, puertoRep: 3002 }, // "id" 0
    { ip: '127.0.0.1', puertoPub: 3003, puertoSub: 3004, puertoRep: 3005 }, // "id" 1
    { ip: '127.0.0.1', puertoPub: 3006, puertoSub: 3007, puertoRep: 3008 }  // "id" 2
];
// Arreglo paralelo con la ocupacion por broker
let contadorTopicosPorBroker = [0, 0, 0];

// Guarda las solicitudes pendientes
var pendingRequests = {};

// ---------------------------------------- Main ------------------------------------------
// Se inicia la escucha 
{
    obtieneBrokersIniciales();
}

function obtieneBrokersIniciales(){
    // Obtengo el de heartbeat

    let mensaje = {
        idPeticion: generateUUID(),
        accion: COD_ADD_TOPICO_BROKER,
        topico: topico
    }
    let brokerHeartbeat = brokerIpPuerto[0];
    let brokerHeartbeatIpPuerto = `tcp://${brokerHeartbeat.ip}:${brokerHeartbeat.puertoRep}`;
    reqSocket.connect(brokerHeartbeatIpPuerto);

    // TODO: Obtener el de message/all

    // TODO: Una vez que las dos conexiones estén confirmadas,
    repSocket.bind(`tcp://${ipCoordinador}:${puertoCoordinador}`);
}

function getBrokerMenosTopicos() {
    let minCantTopicos = Math.min(...contadorTopicosPorBroker);
    return contadorTopicosPorBroker.indexOf(minCantTopicos);
}

//Envia respuesta al cliente del broker, del topico solicitado
function enviarBrokerSegunTopicoExistente(topico,isPub){
    
    let idBroker = topicoIdBroker[topico];
    let ipPuertoBroker = brokerIpPuerto[idBroker];


    let respuesta = {
        ip: ipPuertoBroker.ip,
        puerto: isPub ? ipPuertoBroker.puertoPub : ipPuertoBroker.puertoSub,
        topico: topico
    }

    repSocket.send(JSON.stringify(respuesta));
}

function enviarBrokerSegunTopicoNoExistente(topico,isPub){
    let mensaje = {
        idPeticion: generateUUID(),
        accion: COD_ADD_TOPICO_BROKER,
        topico: topico
    }
    // TODO: Hacer esto porque no está hecho
    let broker = getBrokerMenosTopicos();
    reqSocket.connect(); // ?
    reqSocket.send(JSON.stringify(mensaje))
    // fin del todo
}
       
reqSocket.on('message',function(responseJSON){ //Cuando el broker responde la solicitud de creacion del topico
    let response = JSON.parse(responseJSON);
    // Respuesta exitosa
    if(response && response.exito){
        if(pendingRequests.hasOwnProperty(response.requestId)){
            topicoIdBroker[topico] = idBrokerMenosTopicos;
            contadorTopicosPorBroker[idBrokerMenosTopicos]++;// TODO PREGUNTAR peligro concurrencia
            
            //nadie lo tiene, se lo asignamos a alguien
            
            //avisarle al topico que se le asigno (tiene que confirmar?)  
            enviarBrokerSegunTopicoExistente(topico,isPub);
        }
        else {
            // TODO: Llegó una rta de una request que no hice!
        }
    }
    else {
        // TODO: Mandar respuesta al cliente de que no hubo exito.
    }
})

repSocket.on('message', function (requestJSON) { //un cliente quiere saber donde estan 1 o 3 topicos
    // Paso a objeto la request
    let request = JSON.parse(requestJSON);

    // Se deriva a la funcion que procesa la solicitud según corresponda
    switch (request.accion) {
        case COD_PUB: 
            procesarSolicitudPublicacion(request)
            break;
        case COD_ALTA_SUB: 
            procesarAltaCliente(request);
            break;
        default:
            console.error("ERROR 4503: codigo binario no binario llegad esperado invalido de operacion en la solicitud");
        //algo salio mal esto no tendria que estar aca.
    }
    console.log("Received request: [", request, "]");
});

// TODO: Terminar
// Funciones que procesan las solictudes
function procesarSolicitudPublicacion(request){
    // Si se tiene ese topico asignado a un broker, se envía directamente
    if (topicoIdBroker.hasOwnProperty(topico)) {
        enviarBrokerSegunTopicoExistente(topico,isPub);
    } 
    // De caso contrario, es necesario dar de alta el nuevo topico
    else {
        enviarBrokerSegunTopicoNoExistente(topico,isPub);
    }
}

function procesarAltaCliente(request){
    //mandamos los 3 juntos xq una request admite una sola reply (es el protocolo de ZMQ)
    let mensaje = [getRespuestaTopico('heartbeat',false),getRespuestaTopico(request.topico,false),getRespuestaTopico('message/all',false)]; 
    asignarTopico();
    repSocket.send(JSON.stringify(mensaje));
}



// TO DO: Abstraer a global.js
// Genera UUID a fin de ser utilizado como ID de mensaje.
function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
}
