function initMain() {
    if (!(App.pageLoaded && App.mainTemplateLoaded && App.infoTemplateLoaded && App.errorTemplateLoaded)) {
        // not ready yet
        return;
    }
    App.producer = _.extend(App.producer, {
        $results: $('#sent-results-row'),
        $schemas: $('#schema'),
        $schemaVersions: $('#schema-version'),
        $aceMessage: ace.edit("ace-message"),
        $aceSchema: ace.edit("ace-schema"),
        $topicsSelect: $('#topic-dd')
    });

    $('#send-message-btn').click(function(e) {
        e.preventDefault();
        sendMessage();
    });
    $('#refresh-schemas-btn').click(function(e) {
        e.preventDefault();
        refreshSchemas();
        refreshTopics();
    });
    refreshSchemas();
    App.producer.$schemas.change(handleNewSchema);
    App.producer.$schemaVersions.change(handleNewVersion);
    App.producer.$topicsSelect.change(function() {
        var val = App.producer.$topicsSelect.val();
        // TODO: regex check
//                    if (val.matches(/[\w-]+/)) {
        $('#topic').val(val);
//                    }
    });
    refreshTopics();

    App.producer.$aceMessage.setTheme("ace/theme/chrome");
    App.producer.$aceMessage.getSession().setMode("ace/mode/json");
    App.producer.$aceSchema.setTheme("ace/theme/chrome");
    App.producer.$aceSchema.getSession().setMode("ace/mode/json");
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

function sendMessage() {
    var req = buildRequest();
    $.ajax({
        type: "POST",
        url: App.contextPath + "/api/avro/publish?count=" + getCount(),
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
            schemaRegistryUrl: $('#schemaurl').val(),
            schema: $('#schema').val(),
            rawSchema: App.producer.$aceSchema.getValue(),
            topic: $('#topic').val()
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

function refreshSchemas() {
    var url = App.contextPath + "/api/schemas",
        schemaUrl = $('#schemaurl').val();
    if (schemaUrl !== '') {
        url += "?url=" + schemaUrl;
    }
    var $schemas = App.producer.$schemas;
    $.get(url)
        .done(function(data) {
            $schemas.html('<option value="null">**Select Value**</option>');
            _.each(data, function(schemaName) {
                $schemas.append('<option>' + schemaName + '</option>');
            });
        })
        .fail(function(err) {
            $schemas.html('<option value="null">**Invalid Schema Url**</option>');
        });
}

function handleNewSchema() {
    var name = App.producer.$schemas.val();
    if (name !== 'null') {
        $.get(App.contextPath + '/api/schemas/' + name, function(data) {
            var curr = data.currSchema,
                v = curr.version,
                rawSchema = JSON.stringify(JSON.parse(curr.schema), null, 2);

            App.producer.$schemaVersions.html('<option value="' + v + '">' + v + ' (Current)</option>');
            _.each(data.versions, function(ver) {
                App.producer.$schemaVersions.append('<option>' + ver + '</option>');
            });

            App.producer.$aceSchema.setValue(rawSchema);
        });
    }
}

function handleNewVersion() {
    var name = App.producer.$schemas.val(),
        ver = App.producer.$schemaVersions.val();
    if (!!name && name !== '' && !!ver && ver !== '') {
        $.get(App.contextPath + '/api/schemas/' + name + "/" + ver, function(data) {
            var rawSchema = JSON.stringify(JSON.parse(data.schema), null, 2);
            App.producer.$aceSchema.setValue(rawSchema);
        });
    }
}
