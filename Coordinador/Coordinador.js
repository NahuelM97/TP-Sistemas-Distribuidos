const zmq = require('zeromq');
const globals = require('../Global/Globals');

// socket to talk to clients
var repSocket = zmq.socket('rep');
var reqSocket = zmq.socket('req');


const ipCoordinador = `127.0.0.1`;
const puertoCoordinador = 1234;


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
var pendingRequests = {}; //TODO hacer esto porque no esta hecho
// aca van las request en el orden en que llegaron tipo cola

// ---------------------------------------- Main ------------------------------------------
// Se inicia la escucha 
{
    obtieneBrokersIniciales();
}

function obtieneBrokersIniciales(){
    let reqSocketInit = zmq.socket('req');

    reqSocketInit.on('message', cbSocketInit);

    enviarAsignacionTopicoBroker(reqSocketInit, 'heartbeat', false);
    enviarAsignacionTopicoBroker(reqSocketInit, 'message/all', false);

    reqSocket.on('message', cbAsignaTopicoBroker);
    repSocket.on('message', cbRespondeRequest);
}

function cbAsignaTopicoBroker(responseJSON) { //Cuando el broker responde la solicitud de creacion del topico
    let response = JSON.parse(responseJSON);
    // Respuesta exitosa
    if (response && response.exito) {
        if (pendingRequests.hasOwnProperty(response.requestId)) {
            topicoIdBroker[topico] = idBrokerMenosTopicos;
            contadorTopicosPorBroker[idBrokerMenosTopicos]++;// TODO PREGUNTAR peligro concurrencia

            //nadie lo tiene, se lo asignamos a alguien

            //avisarle al topico que se le asigno (tiene que confirmar?)  
            

            switch (request.accion) {
                case globals.COD_PUB:
                    enviarBrokerSegunTopicoExistente(topico, isPub);
                    break;
                case globals.COD_ALTA_SUB:
                    enviarDatosAlta(topico, false); //TODO preguntar en que contexto de ejecucion se ejecutan los callbacks
                    break;
                default:
            }
        }
        else {
            // TODO: Llegó una rta de una request que no hice!
        }
    }
    else {
        // TODO: Mandar respuesta al cliente de que no hubo exito.
    }
}

function cbRespondeRequest(requestJSON) { //un cliente quiere saber donde estan 1 o 3 topicos
    // Paso a objeto la request
    let request = JSON.parse(requestJSON);

    // Se deriva a la funcion que procesa la solicitud según corresponda
    switch (request.accion) {
        case globals.COD_PUB:
            procesarSolicitudPublicacion(request)
            break;
        case globals.COD_ALTA_SUB:
            procesarAltaCliente(request);
            break;
        default:
            console.error("ERROR 4503: codigo binario no binario llegad esperado invalido de operacion en la solicitud");
        //algo salio mal esto no tendria que estar aca.
    }
    console.log("Received request: [", request, "]");
}




// asignamos los topicos heartbeat y message/all antes de empezar a atender request
function cbSocketInit() {
    //Cuando el broker responde la solicitud de creacion del topico
    let response = JSON.parse(responseJSON);

    if (response && response.exito) {
        topicoIdBroker[topico] = idBrokerMenosTopicos;
        contadorTopicosPorBroker[idBrokerMenosTopicos]++;
        if (getCantKeys(topicoIdBroker) == 2) { // ya se asignaron heartbeat y message/all a un broker.
            repSocket.bind(`tcp://${ipCoordinador}:${puertoCoordinador}`); // aca empieza a escuchar requests
        }
    }
    else {
        // TODO: Mandar respuesta al cliente de que no hubo exito.
    }
}





function getBrokerMenosTopicos() {
    let minCantTopicos = Math.min(...contadorTopicosPorBroker);
    return contadorTopicosPorBroker.indexOf(minCantTopicos);
}

// Envia respuesta al cliente del broker, del topico solicitado porque ya estaba asignado a un broker
function enviarBrokerSegunTopicoExistente(topico,isPub){
    
    let idBroker = topicoIdBroker[topico];
    let ipPuertoBroker = brokerIpPuerto[idBroker];


    let respuesta = {
        ip: ipPuertoBroker.ip,
        puerto: isPub ? ipPuertoBroker.puertoSub : ipPuertoBroker.puertoPub, // me tengo que comunicar con la parte opuesta del protocolo
        topico: topico
    }

    repSocket.send(JSON.stringify(respuesta));
}

// le avisamos a un broker que tiene un topico nuevo asignado
function enviarAsignacionTopicoBroker(socket,topico,isPub){
    let mensaje = {
        idPeticion: globals.generateUUID(),
        accion: globals.COD_ADD_TOPICO_BROKER,
        topico: topico
    }

    let broker = getBrokerMenosTopicos();
    socket.connect(`tcp://${broker.ip}:${broker.puertoRep}`); 
    socket.send(JSON.stringify(mensaje));
    
}
       


// TODO: Terminar
// Funciones que procesan las solictudes
function procesarSolicitudPublicacion(request){
    // Si se tiene ese topico asignado a un broker, se envía directamente
    if (topicoIdBroker.hasOwnProperty(topico)) {
        enviarBrokerSegunTopicoExistente(topico,isPub);
    } 
    // De caso contrario, es necesario dar de alta el nuevo topico
    else {
        enviarAsignacionTopicoBroker(reqSocket, topico,isPub);
    }
}

function procesarAltaCliente(request){
    //mandamos los 3 juntos xq una request admite una sola reply (es el protocolo de ZMQ)
    let mensaje = [getRespuestaTopico('heartbeat',false),getRespuestaTopico(request.topico,false),getRespuestaTopico('message/all',false)]; 
    asignarTopico();
    repSocket.send(JSON.stringify(mensaje));
}




