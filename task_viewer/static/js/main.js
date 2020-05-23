// https://github.com/steelkiwi/aiohttp_test_chat

const TaskStatus = {"INACTIVE": 0, "WAITING": 1, "IN_PROGRESS": 2, "COMPLETED": 3, "FAILED": 4};
Object.freeze(TaskStatus);

const EventType = {"MESSAGE": 0, "TASK": 1, "CMD": 2};
Object.freeze(EventType);

const LogLevel = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "ERROR": 4, "SUCCESS": 5};
Object.freeze(LogLevel);

const CMDType = {"OK": 0, "CLOSE_TASK": 1, "NOTIFY_TASK_CLOSED": 2, "LOAD_LOG": 3};
Object.freeze(CMDType);

const TaskStatusToLogLevel = [2, 2, 2, 2, 4];
const LogLevelToBSClass = ["text-muted", "text-muted", "text", "text-warning", "text-danger", "text-success"];

const RPCStatusToText = ["inactive", "waiting", "in progress", "completed", "failed"];

const EVENT_INCREMENT = 30
var sock
var min_event_id = Number.MAX_SAFE_INTEGER
var username = "undefined" // TODO

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
function updateSubTask(parent, obj) {
    let subTaskBlock = document.getElementById(obj.uuid);
    if(!subTaskBlock)
    {
        subTaskBlock = htmlToElement(`<a id="${obj.uuid}" class="list-group-item"></a>`);
        subTaskBlock.innerHTML = `<h5 class="list-group-item-heading"> ${obj.name}
<small class="message">${obj.msg}</small><span class="badge">${RPCStatusToText[obj.status]}</span></h5>`;
        parent.append(subTaskBlock);
    }

    subTaskBlock.querySelector('.message').innerText = obj.msg;
    subTaskBlock.querySelector('.badge').innerText = RPCStatusToText[obj.status];
    updateProgressBar(subTaskBlock, obj);
}

// fill task block (create if need)
function updateTaskBlock(parent, obj, show_close_btn, show_uuid) {

    let taskBlock = document.getElementById(obj.uuid);
    if(!taskBlock) {

        taskBlock = htmlToElement(`<div id="${obj.uuid}" class="task-block"></div>`);
        taskBlock.append(prepareTaskHeader(obj, show_uuid));
        if(obj.id == 0){
            parent.insertBefore(taskBlock, parent.firstChild);
        } else {
            parent.append(taskBlock);
        }
    }
    updateTaskHeader(taskBlock, obj);
    if(show_close_btn)
        updateCloseBtn(taskBlock, obj);

    list_group = taskBlock.querySelector(".list-group")
    //if(obj.status < TaskStatus.COMPLETED && obj.steps.length > 0){
    if(obj.steps.length > 0){
        if(!list_group){
            list_group = htmlToElement('<div class="list-group"></div>');
            taskBlock.append(list_group);
        }
        let i = 0;
        for(;i < obj.steps.length; i++){
            updateSubTask(list_group, obj.steps[i]);
        }
    }
    //else
    //{
    //    if(list_group)
    //        list_group.remove();
    //}
}

function prepareMsgElement(created, msg, level, temporary=false){
    let obj = document.createElement('p');
    let opts ={hour12: false};
    let class_name = LogLevelToBSClass[level]
    if(temporary){
        class_name += ' temporary'
    }
    obj.setAttribute('class', class_name);
    obj.innerText = `[${created.toLocaleTimeString('en-US', opts)}] ${msg}`;
    return obj;
}

function prepareTaskHeader(task, show_uuid){

    level = TaskStatusToLogLevel[task.status];

    let obj = document.createElement('p');

    let date = new Date(task.created);
    let opts ={hour12: false};

    if(show_uuid)
        uuid_text = ' ' + task.uuid.slice(0, 8);
    else
        uuid_text = '';

    return htmlToElement(
    `<p class="task-header ${LogLevelToBSClass[level]}">
    [${date.toLocaleTimeString('en-US', opts)} ${date.toLocaleDateString()}] ${task.name}
    ${uuid_text}: <span class="task-message">${task.message}</span>
    </p>`);
}

function updateTaskHeader(taskBlock, task){

    level = TaskStatusToLogLevel[task.status];
    p = taskBlock.querySelector('.task-header');
    p.setAttribute('class', 'task-header ' + LogLevelToBSClass[level]);

    span = taskBlock.querySelector('.task-message');
    span.innerText = task.message;
}

function updateCloseBtn(taskBlock, obj){
    let closeBtn = document.getElementById(`close_${obj.uuid}`);
    if(obj.status == TaskStatus.IN_PROGRESS || obj.status == TaskStatus.WAITING)
    {
        if(!closeBtn) {
            closeBtn =
            htmlToElement(`<input class='btn btn-default btn-xs' type='submit' id='close_${obj.uuid}' value='Close'>`);
            closeBtn.onclick = function() { onCloseBtnClick(obj.uuid); };
            taskBlock.children[0].append(closeBtn);

        }
    }
    else if(closeBtn) {
        closeBtn.remove();
    }
}

// show message in div#subscribe
function showMessage(message) {
    let messageElem = document.getElementById('subscribe');
    messageElem.insertBefore(prepareMsgElement(new Date(), message, LogLevel.WARN, true), messageElem.firstChild);
}

function showLogMessage(obj) {
    let messageElem = document.getElementById('subscribe');
    if(obj.id == 0) {
        messageElem.insertBefore(prepareMsgElement(new Date(obj.created), obj.msg, obj.level), messageElem.firstChild);
    } else {
        messageElem.append(prepareMsgElement(new Date(obj.created), obj.msg, obj.level));
    }
}

function updateTaskDescription(obj) {
    let messageElem = document.getElementById('subscribe');
    updateTaskBlock(messageElem, obj, true, true);
}

function updateCMDDescription(obj) {
    let messageElem = document.getElementById('subscribe');
    updateTaskBlock(messageElem, obj, false, false);
}

function showLog(json_data) {
//    console.log('data: ', json_data)
    let obj = JSON.parse(json_data);
    if(obj.id > 0 && min_event_id > obj.id) {
        min_event_id = obj.id;
    }
    if(obj.type == EventType.MESSAGE)
        showLogMessage(obj);
    else if(obj.type == EventType.TASK)
        updateTaskDescription(obj);
    else if(obj.type == EventType.CMD)
        updateCMDDescription(obj);
    else
        showMessage('Error: unknown log type, message: ' + json_data);
    //window.scrollTo(0,document.body.scrollHeight);
}

function onCloseBtnClick(task_id) {
    sock.send(JSON.stringify({
    'cmd': CMDType.CLOSE_TASK,
    'request_id': task_id,
    'username': username
    }));
}

function sendMessage(){
    let msg = $('#message');
    sock.send(msg.val());
    msg.val('').focus();
}

function sendMessage(msg){
    sock.send(msg)
}

function loadEvents(event_increment){
    sock.send(JSON.stringify({
    'cmd': CMDType.LOAD_LOG,
    'count': event_increment,
    'less_than': min_event_id
    }));
}


$(document).ready(function(){

    function connect() {

        try{
            sock = new WebSocket('ws://' + window.location.host + '/ws');
        }
        catch(err){
            sock = new WebSocket('wss://' + window.location.host + '/ws');
        }

        sock.onopen = function(){
            showMessage('Connection to server started');
            if(min_event_id == Number.MAX_SAFE_INTEGER) {
                loadEvents(EVENT_INCREMENT);
            } else {
                loadEvents(0)
            }
        };

        // income message handler
        sock.onmessage = function(event) {
            if(event.data == 'ready') {
                $('.temporary').remove();
//                window.scrollTo(0, document.body.scrollHeight);
            }
            else {
                showLog(event.data);
            }
        };

        sock.onclose = function(event){
            if(event.wasClean){
                showMessage('Clean connection end')
            }else{
                console.log('Connection broken. Reconnect will be attempted in 1 second.', event.reason);
                showMessage('Connection broken. Reconnect will be attempted in 1 second.', event.reason)

                setTimeout(function() {
                    connect();
                }, 1000);
            }
        };

        sock.onerror = function(error){
            console.error('Socket encountered error: ', error.message, 'Closing socket');
            sock.close()
        };
    };

    connect();

    // send message from form
    $('#submit').click(function() {
        sendMessage();
    });

    $('#message').keyup(function(e){
        if(e.keyCode == 13){
            sendMessage();
        }
    });

    $('#signout').click(function(){
        window.location.href = "signout";
    });

    $('#loadMore').click(function(){
        loadEvents(EVENT_INCREMENT);
    });
});
