// https://habr.com/ru/post/200866/

$(document).ready(function(){
    try{
        var sock = new WebSocket('ws://' + window.location.host + '/ws');
    }
    catch(err){
        var sock = new WebSocket('wss://' + window.location.host + '/ws');
    }

    // fill sub task block
    function updateSubTask(taskBlock, obj) {
        let subTaskBlock = document.getElementById(obj.uuid);
        if(!subTaskBlock)
        {
            subTaskBlock = document.createElement('div')
            subTaskBlock.setAttribute('id', obj.uuid)
            subTaskBlock.append(document.createElement('p'))
            subTaskBlock.append(document.createElement('p'))
            taskBlock.append(subTaskBlock)
        }
        subTaskBlock.childNodes[0].innerText = `uuid: ${obj.uuid}, name: ${obj.name}`
        subTaskBlock.childNodes[1].innerText = `progress: ${obj.progress}, message: ${obj.msg}`
    }

    // fill task block (create if need)
    function updateTaskBlock(parent, obj) {
        let taskBlock = document.getElementById(obj.uuid),
            date = new Date(),
            options = {hour12: false};
        if(!taskBlock) {
            taskBlock = document.createElement('div');
            taskBlock.setAttribute('id', obj.uuid);
            taskBlock.setAttribute('class', 'container')
            parent.append(taskBlock);
        }

        taskBlock.innerText = '[' + date.toLocaleTimeString('en-US', options) + '] ' + obj.name;
        let i = 0
        for(;i < obj.steps.length; i++){
            updateSubTask(taskBlock, obj.steps[i]);
        }
    }

    // show message in div#subscribe
    function showMessage(message) {
        let messageElem = $('#subscribe'),
            height = 0,
            date = new Date();
            options = {hour12: false};
        messageElem.append($('<p>').html('[' + date.toLocaleTimeString('en-US', options) + '] ' + message + '\n'));
        messageElem.find('p').each(function(i, value){
            height += parseInt($(this).height());
        });

        messageElem.animate({scrollTop: height});
    }

    function updateTaskDescription(json_data) {
        let obj = JSON.parse(json_data);
        //console.log(typeof(obj));
        let messageElem = $('#subscribe'),
            height = 0;
        updateTaskBlock(messageElem, obj)
        messageElem.find('div').each(function(i, value){
            height += parseInt($(this).height());
        });

        messageElem.animate({scrollTop: height});
    }

    function sendMessage(){
        let msg = $('#message');
        sock.send(msg.val());
        msg.val('').focus();
    }

    sock.onopen = function(){
        showMessage('Connection to server started')
    }

    // send message from form
    $('#submit').click(function() {
        sendMessage();
    });

    $('#message').keyup(function(e){
        if(e.keyCode == 13){
            sendMessage();
        }
    });

    // income message handler
    sock.onmessage = function(event) {
      updateTaskDescription(event.data);
    };

    $('#signout').click(function(){
        window.location.href = "signout"
    });

    sock.onclose = function(event){
        if(event.wasClean){
            showMessage('Clean connection end')
        }else{
            showMessage('Connection broken')
        }
    };

    sock.onerror = function(error){
        showMessage(error);
    }
});
