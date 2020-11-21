const repl = require('repl');

// Wide Area System of Asynchronous Posts

const COMMAND_CHAR = '';

console.clear();
console.log('------------------------------------------------------------------------------');
console.log('|                                                                            |');
console.log('|                              Welcome to WASAP                              |');
console.log('|                  Wide Area Synchronous-Asynchronous Posts                  |');
console.log('|                                                                            |');
console.log('------------------------------------------------------------------------------');
console.log('');
console.log('');
console.log('');
console.log('Bienvenido, utilice ' + COMMAND_CHAR +'ayuda para ver los comandos o '+ COMMAND_CHAR +'salir para salir.');
console.log('');
console.log('');
console.log('');


repl.start({prompt:'WASAP\\Nahuel> ',eval:evaluar,writer:writer});


function evaluar(cmd, context, filename, callback){
    callback(null,cmd);
}

function writer(unparsedInput){
    let inputChiquito = unparsedInput.toString().replace('\n','');
    let input = unparsedInput.toString().replace('\n','').toUpperCase().trim();
    
    if(input == COMMAND_CHAR +'AYUDA'){
        return 'En terminos de ayuda, no hay ayuda.';
    }
    if(input == COMMAND_CHAR +'SALIR'){
        console.log('¡Adiós!');
        return process.exit();
    }
    if(input == COMMAND_CHAR +'CARTA'){
        return carta();
    }
    return "No sé como interpretar este comando: " + inputChiquito;
}

var carta = function () {
    var m = [ "♥", "♦", "♣", "♠" ];
    return m[Math.floor(Math.random()*m.length)];
};