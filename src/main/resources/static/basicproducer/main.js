function initMain() {
    if (!(App.pageLoaded && App.mainTemplateLoaded && App.infoTemplateLoaded && App.errorTemplateLoaded)) {
        // not ready yet
        return;
    }
    App.producer = _.extend(App.producer, {
        $results: $('#sent-results-row'),
        $aceMessage: ace.edit("ace-message"),
        $topicsSelect: $('#topic-dd'),
        $serializerSelect: $('#serializer-dd')
    });

    $('#send-message-btn').click(function(e) {
        e.preventDefault();
        sendMessage();
    });
    App.producer.$topicsSelect.change(function() {
        var val = App.producer.$topicsSelect.val();
        // TODO: regex check
//                    if (val.matches(/[\w-]+/)) {
        $('#topic').val(val);
//                    }
    });
    refreshTopics();
    
    refreshSerializers();

    App.producer.$aceMessage.setTheme("ace/theme/kuroir");
    App.producer.$aceMessage.getSession().setMode("ace/mode/text");
}

function getKafkaHost() {
    var kafkaHost = $('#kafkahost').val();
    return kafkaHost !== "" ? kafkaHost : undefined;
}

function refreshTopics() {
    $.get(App.contextPath + "/api/topics", {"kafka-url": getKafkaHost()}, handleNewTopics);
}

function handleNewTopics(data) {
    var $topicsSelect = App.producer.$topicsSelect;
    $topicsSelect.html("<option>Select an existing topic or enter a new one</option>");
    _.each(data, function(topicName) {
        $topicsSelect.append("<option>" + topicName + "</option>");
    });
}

function refreshSerializers() {
    $.get(App.contextPath + "/api/manager/serializers", handleSerializers);
}

function handleSerializers(data) {
    var $serializerSelect = App.producer.$serializerSelect;
    $serializerSelect.html('');
    _.each(data.content, function(info) {
        $serializerSelect.append("<option value='" + info.id + "'>" + info.name + "</option>");
    });
}

function sendMessage() {
    var req = buildRequest();
    $.ajax({
        type: "POST",
        url: App.contextPath + "/api/kafka/publish?count=" + getCount(),
        headers: {
            "Content-Type": "application/json",
            Accept: "application/json;charset=UTF-8"
        },
        data: JSON.stringify(req),
        success: handleResponse
    });
    $('#send-message-btn').addClass("disabled");
}

function buildRequest() {
    var req = {
        meta: {
            kafkaUrl: $('#kafkahost').val(),
            topic: $('#topic').val(),
            serializerId: App.producer.$serializerSelect.val()
        },
        rawMessage: App.producer.$aceMessage.getValue(),
        key: $('#message-key').val()
    };
    if (req.meta.kafkaUrl === "") {
        req.meta.kafkaUrl = null;
    }
    if (req.meta.schemaRegistryUrl === "") {
        req.meta.schemaRegistryUrl = null;
    }
    return req;
}

function getCount() {
    var count = $('#count').val();
    if (count !== '') {
        count = parseInt(count);
        if (count !== NaN && count > 0 && count < 10000) {
            return count;
        }
    }
    return 1;
}

function handleResponse(data) {
    var timeText = moment().format('LTS'),
        template = null;
    if (!!data && !!data.sent && data.sent && !!data.success && data.success) {
        // request was successful
        template = App.producer.sentInfoTemplate;
    } else {
        data = {};
        template = App.producer.sentErrorTemplate;
    }
    data.timeText = timeText;
    App.producer.$results.html(template(data));
    $('#send-message-btn').removeClass("disabled");
    window.scrollTo(0, document.body.scrollHeight);
}
