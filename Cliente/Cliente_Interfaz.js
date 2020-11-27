const repl = require('repl');
const CLIENT = require('./Cliente');

// Wide Area System of Asynchronous Posts

const COMMAND_CHAR = '';


var isLogged = false;
var username;
let replStart;

// Prog princ
{
    console.clear();
    console.log('');
    console.log('');
    console.log('');
    console.log('   ------------------------------------------------------------------------------');
    console.log('   |                                                                            |');
    console.log('   |                                                                            |');
    console.log('   |                              Welcome to WASAP                              |');
    console.log('   |                  Wide Area Synchronous-Asynchronous Posts                  |');
    console.log('   |                                                                            |');
    console.log('   |                                                                            |');
    console.log('   ------------------------------------------------------------------------------');
    console.log('');
    console.log('');
    console.log('');
    console.log('Bienvenido, escriba ' + COMMAND_CHAR +'ayuda para ver los comandos o '+ COMMAND_CHAR +'salir para salir.');
    console.log('');
    console.log('');
    console.log('');
    
    
    replStart = repl.start({prompt:'WASAP> ',eval:evaluar,writer:writer});
}


// -----------------------------------------------------------------------------
// <Procesamiento de comandos>
// -----------------------------------------------------------------------------

function evaluar(cmd, context, filename, callback){
    callback(null,cmd);
}

function writer(unparsedInput){
    let inputSinProcesar = unparsedInput.toString().replace('\n','');
    let input = unparsedInput.toString().replace('\n','').toUpperCase().trim();
    let command = input.split(' ')[0];
    
    if(command == COMMAND_CHAR +'LOGIN'){
        return commandLogin(inputSinProcesar);
    }
    if(command == COMMAND_CHAR +'AYUDA' || command == COMMAND_CHAR +'H'){ // Comandos de ayuda: Ayuda o H
        return commandAyuda();
    }
    if(command == COMMAND_CHAR +'SALIR'){
        console.log('¡Adiós!');
        return process.exit();
    }

    // comandos que requieren LOGIN
    if(command == COMMAND_CHAR +'ENVIAR'){
        if(!isLogged) {
            return notLoggedMessage();
        }
        return commandEnviar(inputSinProcesar);
    }
    if(command == COMMAND_CHAR +'GRUPO'){
        if(!isLogged) {
            return notLoggedMessage();
        }
        return commandGroup(inputSinProcesar);
    }

    return "No sé como interpretar este comando: " + inputSinProcesar;
}


// Procesa el input AYUDA. Muestra ayuda al usuario
function commandAyuda() {
    return '\n'+
    '   AYUDA \n'+
    '       Muestra lista de comandos.\n\n'+
    '   ENVIAR <message> [-a | -u <user> | -g <group>] \n'+
    '       Envía un mensaje a todos, un usuario, o un grupo.\n\n'+
    '   GRUPO <group> \n'+
    '       Crea o se une a un grupo.\n\n'+
    '   LOGIN <username> \n'+
    '       Se conecta al sistema con un nombre de usuario.\n\n'+
    '   SALIR \n        Salir del sistema.\n\n'+
    '';
}

// Procesa el input ENVIAR.
// Precondición: Input ya está toUpper y trimmed
function commandEnviar(input) {
    let inputArray = input.trim().split(' ');

    switch(inputArray.length) {
        case 0,1:
            return tooFewArgumentsMessage();
        
        case 2: // No se especificó ningún guion-algo. Comportamiento por defecto: se asume -a
            return EnviarMensajeAll(inputArray[1]);
            
        case 3:
            if(inputArray[2].toUpperCase() != '-A') {
                return invalidFormatMessage();
            }
            return EnviarMensajeAll(inputArray[1]);

        case 4:
            if(inputArray[2].toUpperCase() == '-U') {
                return EnviarMensajeUsuario(inputArray[1], inputArray[3]);
            }
            if(inputArray[2].toUpperCase() == '-G') {
                return EnviarMensajeGrupo(inputArray[1], inputArray[3]);
            }
            return tooManyArgumentsMessage();

        default:
            return 
    }
}

// Crea un grupo o, si ya existe, se une a él
function commandGroup(input) {
    let inputArray = input.trim().split(' ');
    if(inputArray.length < 2) {
        return tooFewArgumentsMessage();
    }
    if(inputArray.length > 2) {
        return tooManyArgumentsMessage();
    }
    CLIENT.suscripcionAGrupo(inputArray[1]);
}

function commandLogin(input) {
    let inputArray = input.trim().split(' ');
    if(isLogged) {
        return 'Ya estás logueado, ' + username + '!';
    }
    if(inputArray.length < 2) {
        return tooFewArgumentsMessage();
    }
    if(inputArray.length > 2) {
        return tooManyArgumentsMessage();
    }
    if(inputArray[1].toUpperCase() == 'ALL') {
        return invalidNameMessage();
    }
    else {
        username = inputArray[1];
        replStart.setPrompt('WASAP\\'+username+'>');
        isLogged = true;
        CLIENT.init(username); // Hace el alta del cliente en el sistema (Conexion)
    }
}


function EnviarMensajeAll(contenido) {
    return CLIENT.EnviarMensajeAll(contenido);
}

function EnviarMensajeUsuario(contenido,user) {
    return CLIENT.EnviarMensajeUsuario(contenido,user);
}

function EnviarMensajeGrupo(contenido,group) {
    return CLIENT.EnviarMensajeGrupo(contenido,group);
}

// -----------------------------------------------------------------------------
// </Procesamiento de comandos>
// -----------------------------------------------------------------------------


// -----------------------------------------------------------------------------
// <Mensajes de error>
// -----------------------------------------------------------------------------

function notLoggedMessage() {
    return 'Debe loguearse para utilizar el envío de mensajes. Utilice ' + COMMAND_CHAR + 'login para loguearse.';
}

function invalidNameMessage() {
    return 'Nombre inválido. Puede utilizar todos los nombres posibles del universo, excepto ese.';
}

function invalidFormatMessage() {
    return 'Formato inválido. Utilice ' + COMMAND_CHAR + 'ayuda para ver los comandos.';
}

function tooManyArgumentsMessage() {
    return 'Demasiados argumentos recibidos. Utilice ' + COMMAND_CHAR + 'ayuda para ver los comandos.';
}

function tooFewArgumentsMessage() {
    return 'Debe especificar al menos un argumento. Utilice el comando ' + COMMAND_CHAR + 'ayuda para ver los comandos.';
}

// -----------------------------------------------------------------------------
// </Mensajes de error>
// -----------------------------------------------------------------------------