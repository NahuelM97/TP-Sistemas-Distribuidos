
//---------------------------<Codigos de accion para REQREP>----------------------------------------------

//CLIENTE -> COORDINADOR
const COD_PUB = 1; // Cliente publica un nuevo mensaje

const COD_ALTA_SUB = 2; // Cliente se suscribe a un t�pico


//COORDINADOR -> BROKER
const COD_ADD_TOPICO_BROKER = 3;// Coordinador informa al broker un nuevo topico 


//SERVIDOR HTTP -> BROKER
const COD_GET_TOPICOS = 4; // Servidor solicita a todos los sus topicos 

const COD_GET_MENSAJES_COLA = 5;// Servidor solicita a todos los brokers todos sus mensajes

const COD_BORRAR_MENSAJES = 6; // Servidor solicita a un broker que borre sus mensajes

//---------------------------</Codigos de accion para REQREP>----------------------------------------------


//---------------------------<Codigos de error para REQREP>-----------------------------------------------

const COD_ERROR_TOPICO_INEXISTENTE = 1
const COD_ERROR_OPERACION_INEXISTENTE = 2




//---------------------------</Codigos de error para REQREP>-----------------------------------------------


//---------------------------<Prefijos para los topicos>---------------------------------------------------
const TOPIC_DELIMITER = '/';
const GROUP_ID_PREFIX = 'g_';
const MESSAGE_TOPIC_PREFIX = 'message';
const GROUP_TOPIC_PREFIX = MESSAGE_TOPIC_PREFIX + TOPIC_DELIMITER + GROUP_ID_PREFIX;
const HEARTBEAT_TOPIC_NAME = 'heartbeat';
const ID_ALL = 'all';
const MESSAGE_ALL_TOPIC_NAME = MESSAGE_TOPIC_PREFIX + TOPIC_DELIMITER + ID_ALL;

//---------------------------</Prefijos para los topicos>--------------------------------------------------




// retorna una respuesta exitosa con los resultados especificados
function generarRespuestaExitosa(accion, idPeticion, resultados) {

	let respuesta = {
		exito: true,
		accion: accion,
		idPeticion: idPeticion,
		resultados: resultados
	}
	return respuesta;
}


// retorna una respuesta no exitosa con el codigo de error y mensaje especificados
function generarRespuestaNoExitosa(accion, idPeticion, codigoError, mensajeError) {

	let respuesta = {
		exito: false,
		accion: accion,
		idPeticion: idPeticion,
		error:
		{
			codigo: codigoError,
			mensaje: mensajeError
		}
	}
	return respuesta;
}


// Genera UUID a fin de ser utilizado como ID de mensaje.
function generateUUID() {
	return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
		var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
		return v.toString(16);
	});
}


// dado un arreglo asociativo keyvalue retorna la cantidad de keys (elementos) que contiene
function getCantKeys(keyvalue) {
	return Object.keys(keyvalue).length;
}

// dado un arreglo asociativo keyvalue retorna sus keys (claves) que contiene
function getKeys(keyvalue) {
	return Object.keys(keyvalue);
}


module.exports = {
	// constantes
	COD_PUB: COD_PUB,
	COD_ALTA_SUB: COD_ALTA_SUB,
	COD_ADD_TOPICO_BROKER: COD_ADD_TOPICO_BROKER,
	COD_GET_TOPICOS: COD_GET_TOPICOS,
	COD_GET_MENSAJES_COLA: COD_GET_MENSAJES_COLA,
	COD_BORRAR_MENSAJES: COD_BORRAR_MENSAJES,

	COD_ERROR_TOPICO_INEXISTENTE: COD_ERROR_TOPICO_INEXISTENTE,
	COD_ERROR_OPERACION_INEXISTENTE: COD_ERROR_OPERACION_INEXISTENTE,


	TOPIC_DELIMITER: TOPIC_DELIMITER,
	GROUP_ID_PREFIX: GROUP_ID_PREFIX,
	MESSAGE_TOPIC_PREFIX: MESSAGE_TOPIC_PREFIX,
	GROUP_TOPIC_PREFIX: GROUP_TOPIC_PREFIX,
	HEARTBEAT_TOPIC_NAME: HEARTBEAT_TOPIC_NAME,
	ID_ALL: ID_ALL,
	MESSAGE_ALL_TOPIC_NAME: MESSAGE_ALL_TOPIC_NAME,

	// funciones
	generarRespuestaExitosa: generarRespuestaExitosa,
	generarRespuestaNoExitosa: generarRespuestaNoExitosa,
	generateUUID: generateUUID,
	getCantKeys: getCantKeys,
	getKeys: getKeys
}
