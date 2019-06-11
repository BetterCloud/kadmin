function initMain() {
    if (!(App.pageLoaded && App.mainTemplateLoaded && App.messageTemplateLoaded)) {
        // not ready yet
        return;
    }
    var $refreshRate = $('#refresh-rate');
    App.consumer = _.extend(App.consumer, {
        count : 0,
        $refreshRate: $refreshRate,
        consumerConfig: {
            started: false,
            kafkaUrl: null,
            schemaUrl: null,
            topic: null,
            since: null,
            refreshHandle: null,
            refreshRate: $refreshRate.val()
        },
        $messageList: $('#message-list'),
        clipboard: null,
        $topicsSelect: $('#topic-dd'),
        $desSelect: $('#deserializer-dd')
    });

    $('#refresh-rate').change(function(e) {
        var consumerConfig = App.consumer.consumerConfig;
        consumerConfig.refreshRate = $refreshRate.val();
        if (!!consumerConfig.refreshHandle) {
            clearTimeout(consumerConfig.refreshHandle);
        }
        if (consumerConfig.refreshRate > 0 && consumerConfig.started) {
            refresh();
        }
        console.log("Refresh Rate: " + consumerConfig.refreshRate);
    });
    $('#start-consumer-btn').click(function(e) {
        e.preventDefault();
        refresh();
    });
    $('#refresh-topics-btn').click(refreshTopics);
    App.consumer.$topicsSelect.change(function() {
        var val = App.consumer.$topicsSelect.val();
        // TODO: regex check
//                    if (val.matches(/[\w-]+/)) {
        $('#topic').val(val);
//                    }
    });
    if (!!App.defaultTopic && App.des.id && App.des.name) {
        $('#topic').val(App.defaultTopic);
        $('#deserializer-dd').html('<option value="' + App.des.id + '">' + App.des.name + '</option>');
        refresh();
    } else {
        refreshTopics();
        refreshDeserializers();
    }
}


function getKafkaHost() {
    var kafkaHost = $('#kafkahost').val();
    return kafkaHost !== "" ? kafkaHost : undefined;
}

function refreshTopics() {
    $.get(App.contextPath + "/api/topics", {"kafka-url": getKafkaHost()}, handleNewTopics);
}

function handleNewTopics(data) {
    App.consumer.$topicsSelect.html("<option>Select an existing topic or enter a new one</option>");
    _.each(data, function(topicName) {
        App.consumer.$topicsSelect.append("<option>" + topicName + "</option>");
    });
}

function refreshDeserializers() {
    $.get(App.contextPath + "/api/manager/deserializers", handleNewDeserializers);
}

function handleNewDeserializers(data) {
    App.consumer.$desSelect.html('');
    _.each(data.content, function(des) {
        App.consumer.$desSelect.append("<option value='" + des.id + "'>" + des.name + "</option>");
    });
}

function initConfig() {
    var topic = $('#topic').val();
    if (!topic || topic === "") {
        alert("Topic is required. Aborting creating consumer.");
        return false;
    }
    consumerConfig = {
        started: true,
        kafkaUrl: $('#kafkahost').val(),
        schemaUrl: $('#schemaurl').val(),
        topic: $('#topic').val(),
        since: -1,
        autoRefresh: null,
        refreshRate: App.consumer.$refreshRate.val(),
        desClass: App.consumer.$desSelect.val()
    };
    if (consumerConfig.kafkaUrl === "") {
        consumerConfig.kafkaUrl = null;
    }
    if (consumerConfig.schemaUrl === "") {
        consumerConfig.schemaUrl = null;
    }
    App.consumer.consumerConfig = consumerConfig;
    disableForm();
    initMessageList();
    return true;
}

function disableForm() {
    $("#kafkahost").prop("disabled", true);
    $("#schemaurl").prop("disabled", true);
    $("#topic").prop("disabled", true);
    App.consumer.$topicsSelect.prop("disabled", true);
    $("#start-consumer-btn").addClass("disabled");
    App.consumer.$desSelect.prop("disabled", true);
}

function initMessageList() {
    $('#message-list-title').html("Messages - " + consumerConfig.topic);
    var $refresh = $("#refresh-btn");
    $refresh.removeClass("disabled");
    $refresh.click(function() { refresh(true); });
    var $clear = $("#clear-btn");
    $clear.removeClass("disabled");
    $clear.click(truncateList);
    var $dispose = $("#dispose-btn");
    $dispose.removeClass("disabled");
    $dispose.click(disposeConsumer);
    var $permalink = $("#permalink-btn");
    $permalink.removeClass("disabled");
    $permalink.attr("data-clipboard-text", window.location.origin + App.contextPath + "/consumer/topic/" +
        consumerConfig.topic + "/" + consumerConfig.desClass);
    new Clipboard("#permalink-btn");
}

function truncateList() {
    if (!App.consumer.consumerConfig.started) {
        if (!initConfig()) {
            return;
        }
    }
    $.ajax({
        type: "DELETE",
        url: App.contextPath + "/api/manager/consumers/" + App.consumer.consumerConfig.id + "/truncate",
        success: refresh
    });
}

function disposeConsumer() {
    if (!App.consumer.consumerConfig.started) {
        if (!initConfig()) {
            return;
        }
    }
    $.ajax({
        type: "DELETE",
        url: App.contextPath + "/api/manager/consumers/" + App.consumer.consumerConfig.id,
        success: function() {
            window.location.href = App.contextPath;
        }
    });
}

function refresh(manualRefresh) {
    var consumerConfig = App.consumer.consumerConfig;
    if (!consumerConfig.started) {
        if (!initConfig()) {
            return;
        }
    }
    var url = buildUrl();
    consumerConfig.since = new Date().getTime();
    $.get(url, handleResults);
    $("#since-row").html("Updated: " + moment(consumerConfig.since).format('LTS'));
    if (consumerConfig.refreshRate > 0 && !manualRefresh) {
        consumerConfig.refreshHandle = setTimeout(refresh, consumerConfig.refreshRate);
    }
}

function buildUrl() {
    var consumerConfig = App.consumer.consumerConfig,
        url = App.contextPath + "/api/kafka/read/" + consumerConfig.topic + "?deserializerId=" + consumerConfig.desClass + "&";
    if (!!consumerConfig.kafkaUrl) {
        url += "kafkaUrl=" + consumerConfig.kafkaUrl + "&";
    }
    if (!!consumerConfig.schemaUrl) {
        url += "schemaUrl=" + consumerConfig.schemaUrl;
    }
    return url;
}

function handleResults(res) {
    var count = 0,
        consumerConfig = App.consumer.consumerConfig,
        data = res.page;
    App.consumer.consumerConfig.id = res.consumerId;
    if (!!App.consumer.clipboard) {
        App.consumer.clipboard.destroy();
    }
    App.consumer.$messageList.html('');
    document.title = "(" + data.totalElements + ") " + consumerConfig.topic;
    _.each(data.content, function(ele) {
        var html = "";
        ele.uuid = uniqueId();
        ele.writeTimeText = moment(ele.writeTime).format('LTS');
        ele.messageText = "null";
        if (!!ele.message) {
            ele.rawMessage = JSON.stringify(ele.message, null, 2).replace(/([^\\])\\n/g, "$1\n");
            ele.messageText = syntaxHighlight(ele.rawMessage);
        } else {
            ele.rawMessage = "null";
            ele.messageText = "null";
        }
        ele.timestamp = "_" + count++;
        html = App.consumer.messageTemplate(ele);
        App.consumer.$messageList.prepend(html);
    });
    App.consumer.clipboard = new Clipboard('.copy-btn');
}

function uniqueId() {
    return 'xxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
        return v.toString(16);
    });
}

function syntaxHighlight(json) {
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
}
