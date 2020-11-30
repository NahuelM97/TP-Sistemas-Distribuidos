const repl = require('repl');
const CLIENT = require('./Cliente');
const globals = require('../Global/Globals');
const { GROUP_ID_PREFIX } = require('../Global/Globals');

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
        return commandSalir();
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
    '   ENVIAR [-a | -u <user> | -g <group>] <message>\n'+
    '       Envía un mensaje a todos, un usuario, o un grupo.\n\n'+
    '   GRUPO <group> \n'+
    '       Crea o se une a un grupo.\n\n'+
    '   LOGIN <username> \n'+
    '       Se conecta al sistema con un nombre de usuario.\n\n'+
    '   SALIR \n        Salir del sistema.\n\n'+
    '';
}

function commandSalir() {
    if(isLogged){        
        // Esto hace process.exit cuando el ntp termina
        CLIENT.endClientNTP();
    }
    else {   
        process.exit();
    }
    return "¡Adiós!";
}

// Procesa el input ENVIAR.
// Precondición: Input ya está toUpper y trimmed
function commandEnviar(input) {
    let inputArray = input.trim().split(' ');

    // PARSER!
    // WARNING: Puede ser un poco espagueti...

    if( inputArray.length < 2 ){ // 0 o 1
        return tooFewArgumentsMessage();
    } 
    
    let inp1 = inputArray[1]; // Si hay un argumento, este sólo puede ser inputArray[1].
    if( inp1[0] == "-" ){ // la primer letra es un guión
        
        // Caso -A
        if ( inp1.toUpperCase() == "-A" ){

            if(inputArray.length <= 2) {
                return tooFewArgumentsMessage();
            }
            else {
                inputArray.splice(0,2); // Borro el ENVIAR y el -A
                return EnviarMensajeAll(inputArray); 
            }

        }

        // Caso -G
        if ( inp1.toUpperCase() == "-G" ){

            if(inputArray.length <= 3) {
                return tooFewArgumentsMessage();
            }
            else {
                let grupo = inputArray[2];
                inputArray.splice(0,3); // Borro el ENVIAR, -G y GROUPNAME
                return EnviarMensajeGrupo(inputArray, grupo); 
            }

        }

        // Caso -U
        if ( inp1.toUpperCase() == "-U" ){

            if(inputArray.length <= 3) {
                return tooFewArgumentsMessage();
            }
            else {
                let usuario = inputArray[2];
                inputArray.splice(0,3); // Borro el ENVIAR, -U y USERNAME
                return EnviarMensajeUsuario(inputArray,usuario); 
            }

        }

        // Si no es ninguna, mal comando
        return invalidFormatMessage();
    }
    else {
        inputArray.splice(0,1); // Borro el ENVIAR
        return EnviarMensajeAll(inputArray); 
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
    return CLIENT.suscripcionAGrupo(inputArray[1]);
    
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
    if(inputArray[1].toUpperCase() == globals.ID_ALL.toUpperCase() || inputArray[1].toLowerCase().startsWith(globals.GROUP_ID_PREFIX)) {//NECOP
        return invalidNameMessage();
    }
    else {
        username = inputArray[1];
        replStart.setPrompt('WASAP\\'+username+'>');
        isLogged = true;
        CLIENT.init(username); // Hace el alta del cliente en el sistema (Conexion)
        return 'Bienvenido, ' + username;
    }
}


function EnviarMensajeAll(contenido) {
    let mensaje = contenido.join(" ");
    return CLIENT.enviarMensajeAll(mensaje);
}

function EnviarMensajeUsuario(contenido,user) {
    let mensaje = contenido.join(" ");
    return CLIENT.enviarMensajeUsuario(mensaje,user);
}

function EnviarMensajeGrupo(contenido,group) {
    let mensaje = contenido.join(" ");
    return CLIENT.enviarMensajeGrupo(mensaje,group);
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
    return `Nombre inválido. Puede utilizar todos los nombres posibles del universo, excepto ${globals.ID_ALL} y cualquiera que empiece con ${globals.GROUP_ID_PREFIX}.`;
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


// Eventos de CTRL+C y de cerrar ventana (X)
process.on('SIGHUP', function () {
    commandSalir();
});

process.on('SIGINT', function () {
    commandSalir();
});