const portServidorHTTP = 9123;

$('#btnMostrarMensajes').click(GetMensajes);


$("#btnSolicitarTopicos").click(function(){
    //hacer un get al servidor
    let brokerId = $('#ddlIdBroker').val();
    let url = `http://localhost:${portServidorHTTP}/broker/${brokerId}/topics`;

    var XHR = new XMLHttpRequest();
    XHR.addEventListener("load",mostrarTopicos);
    XHR.open("GET", url);
    XHR.send();


    $("#cargandoTopicos").show();
});

$("#ddlIdBroker").change(function(){

    $("#resultadoTopicos").fadeOut();
    $("#divListaMensajes").fadeOut();

});

$("#btnEliminarCola").click(function(){
    let brokerId = $('#ddlIdBroker').val();
    let topicId = $('#selectTopicoId').val();
    let topicoEnc = encodeURIComponent(topicId);
    let url = `http://localhost:${portServidorHTTP}/broker/${brokerId}/topics/${topicoEnc}`;
    
    // let url = `http://localhost:${portServidorHTTP}/`;
    

    var XHR = new XMLHttpRequest();
    XHR.open("DELETE", url);
    console.log(XHR);
    XHR.send();
    
    $('#divListaMensajes').fadeOut();
    $('#ulResultadoMensajes').empty();

});

function topicosError(){
    $("#cargandoTopicos").hide();
    alert("Error al recibir respuesta del servidor");
}

function mostrarTopicos() {
    $("#cargandoTopicos").hide();
    $("#resultadoTopicos").fadeIn();

    console.log(this.responseText);
    let response = JSON.parse(this.responseText);

    document.getElementById('resultadoTopicos').classList.remove('escondido');

    let selectTopicos = document.getElementById("selectTopicoId");
    vaciarListaTopicos(selectTopicos);


    if (response.exito) {
        response.resultados.listaTopicos.forEach(topico => {
            let option = document.createElement("option");
            option.value = topico;
            option.text = topico;
            selectTopicos.add(option);
        });


    }
    else {
        console.log(`Error al solicitar los topicos Codigo: ${response.error.codigo} Descripcion: ${response.error.mensaje}`)
    }
    //agregar 1 elemento a la lista del select



}


function GetMensajes() {
    // Limpia los mensajes existentes
    $('#ulResultadoMensajes').empty();
    $('#divListaMensajes').hide();

    //hacer un get al servidor
    let brokerId = $('#ddlIdBroker').val();
    let topicoId = $('#selectTopicoId').val();
    let topicoEnc = encodeURIComponent(topicoId);
    let url = `http://localhost:9123/broker/${brokerId}/topics/${topicoEnc}`;

    var XHR = new XMLHttpRequest();
    XHR.addEventListener("load", cbMostrarMensajes);
    XHR.open("GET", url);
    XHR.send();
}

function cbMostrarMensajes() {
    console.log(this.responseText); 

    let response = JSON.parse(this.responseText);

    $('#divListaMensajes').fadeIn();

    let ulMensajes = document.getElementById("ulResultadoMensajes");


    if (response.exito) {
        if(response.resultados.mensajes.length == 0){
            let listElement = document.createElement("li");
            let textoLi = "No hay mensajes para mostrar en la cola."
            listElement.appendChild(document.createTextNode(textoLi));
            ulMensajes.appendChild(listElement);
        } else{
            response.resultados.mensajes.forEach(mensaje => {

                let listElement = document.createElement("li");
                let textoLi = getFormattedMessage(mensaje);
                listElement.appendChild(document.createTextNode(textoLi));
                ulMensajes.appendChild(listElement);
            });
        }
        


    }
    else {
        console.log(`Error al solicitar los topicos Codigo: ${response.error.codigo} Descripcion: ${response.error.mensaje}`)
    }

}

function getFormattedMessage(mensaje){
    let formattedFecha = getFormattedDate(mensaje.fecha);
    return `${formattedFecha} - ${mensaje.emisor}: ${mensaje.mensaje}`
}

function getFormattedDate(date){
        let fecha = new Date(date);
        let month = (fecha.getMonth() + 1).toString();
        let day = fecha.getDate().toString();
        let formattedDay = day.length == 1 ? "0" + day : day; // Si el día no empieza en 0 se lo agrega
        let formattedMonth = month.length == 1 ? "0" + month : month; // Si el mes no empieza en 0 se lo agrega
        let formattedHours = fecha.getHours().toString().length == 1 ? "0" + fecha.getHours() : fecha.getHours();
        let formattedMinutes = fecha.getMinutes().toString().length == 1 ? "0" + fecha.getMinutes() : fecha.getMinutes();
        let formattedFecha = formattedDay + '/' + formattedMonth + '/' + fecha.getFullYear() + ' ' + formattedHours + ':' + formattedMinutes;
    return formattedFecha;
}

//vacia el select de los topicos
function vaciarListaTopicos(selectTopicos) {
    for (let i = selectTopicos.options.length; i >= 0; i--)
        selectTopicos.remove(i);
}