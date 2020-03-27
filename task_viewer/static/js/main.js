// https://github.com/steelkiwi/aiohttp_test_chat

const TaskStatus = {"INACTIVE": 0, "WAITING": 1, "IN_PROGRESS": 2, "COMPLETED": 3, "FAILED": 4};
Object.freeze(TaskStatus);

const EventType = {"MESSASGE": 0, "TASK": 1};
Object.freeze(EventType);

const LogLevel = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "SUCCESS": 5};
Object.freeze(LogLevel);

const LogLevelToBSClass = ["text-muted", "text-muted", "text", "text-warning", "text-danger", "text-success"]

const RPCStatusToText = ["inactive", "waiting", "in progress", "completed", "failed"]

/**
 * @param {String} HTML representing a single element
 * @return {Element}
 */
function htmlToElement(html) {
    var template = document.createElement('template');
    html = html.trim(); // Never return a text node of whitespace as the result
    template.innerHTML = html;
    return template.content.firstChild;
}

$(document).ready(function(){
    try{
        var sock = new WebSocket('ws://' + window.location.host + '/ws');
    }
    catch(err){
        var sock = new WebSocket('wss://' + window.location.host + '/ws');
    }

    function updateProgressBar(subTaskBlock, obj) {

        let progress = subTaskBlock.querySelector('.progress');

        if(obj.status == TaskStatus.IN_PROGRESS) {
            if (progress) {
                progress.childNodes[0].setAttribute("style", `width:${Math.floor(obj.progress * 100.0)}%`)
            } else {

                progress = document.createElement('div');
                progress.setAttribute('class', 'progress');
                progress.innerHTML = `<div class="progress-bar" role="progressbar"
                aria-valuemin="0" aria-valuemax="100" style="width:${Math.floor(obj.progress * 100.0)}%">`;
                subTaskBlock.append(progress)
            }
        }
        else if(progress)
            subTaskBlock.removeChild(progress)
    }

    // fill sub task block
    function updateSubTask(taskBlock, obj) {
        let subTaskBlock = document.getElementById(obj.uuid);
        if(!subTaskBlock)
        {
            subTaskBlock = htmlToElement(`<a id="${obj.uuid}" class="list-group-item" href="#"></a>`);
            subTaskBlock.innerHTML = `<h5 class="list-group-item-heading"> ${obj.name}
<small class="message">${obj.msg}</small><span class="badge">${RPCStatusToText[obj.status]}</span></h5>`;
            taskBlock.append(subTaskBlock);
        }

        subTaskBlock.querySelector('.message').innerText = obj.msg;
        subTaskBlock.querySelector('.badge').innerText = RPCStatusToText[obj.status];
        updateProgressBar(subTaskBlock, obj);
    }

    // fill task block (create if need)
    function updateTaskBlock(parent, obj) {

        let taskBlock = document.getElementById(obj.uuid),
            date = new Date(),
            options = {hour12: false};
        if(!taskBlock) {

            taskBlock = htmlToElement(`<div id="${obj.uuid}" class="task-block"></div>`);
            taskBlock.append(prepareMsgElement(obj.name, LogLevel.INFO));
            taskBlock.append(htmlToElement('<div class="list-group"></div>'));
            parent.append(taskBlock)
        }
        updateCloseBtn(taskBlock, obj);
        let i = 0;
        for(;i < obj.steps.length; i++){
            updateSubTask(taskBlock.childNodes[1], obj.steps[i]);
        }
    }

    function prepareMsgElement(msg, level){
        let obj = document.createElement('p');
        let date = new Date();
        let opts ={hour12: false};
        obj.setAttribute('class', LogLevelToBSClass[level]);
        obj.innerText = `[${date.toLocaleTimeString('en-US', opts)}] ${msg}`;
        return obj;
    }

    function updateCloseBtn(taskBlock, obj){
        let closeBtn = document.getElementById(`close_${obj.uuid}`);
        if(obj.status == TaskStatus.IN_PROGRESS)
        {
            if(!closeBtn) {
                taskBlock.children[0].append(
                    htmlToElement(`<input class='btn btn-default btn-xs' type='submit' id='close_${obj.uuid}' value='Close'>`))
            }
        }
        else if(closeBtn) {
            closeBtn.remove();
        }
    }

    // show message in div#subscribe
    function showMessage(message) {
        let messageElem = document.getElementById('subscribe');
        messageElem.append(prepareMsgElement(message, LogLevel.WARN));
    }

    function showLogMessage(obj) {
        let messageElem = document.getElementById('subscribe');
        messageElem.append(prepareMsgElement(obj.msg, obj.level))
    }

    function updateTaskDescription(obj) {

        //console.log(typeof(obj));
        let messageElem = document.getElementById('subscribe');
        updateTaskBlock(messageElem, obj);
    }

    function showLog(json_data) {
        let obj = JSON.parse(json_data);
        if(obj.type == EventType.MESSASGE)
            showLogMessage(obj);
        else if(obj.type == EventType.TASK)
            updateTaskDescription(obj);
        else
            showMessage('Error: unknown log type');
        window.scrollTo(0,document.body.scrollHeight);
    }

    function sendMessage(){
        let msg = $('#message');
        sock.send(msg.val());
        msg.val('').focus();
    }

    sock.onopen = function(){
        showMessage('Connection to server started')
    };

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
        showLog(event.data);
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
