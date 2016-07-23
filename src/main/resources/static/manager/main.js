function initMain() {
    if (!(App.pageLoaded && App.mainTemplateLoaded && App.consumerTemplateLoaded && App.producerTemplateLoaded)) {
        // not ready yet
        return;
    }
    App.manager = _.extend(App.manager, {
        $consumerUpdated: $("#consumer-list-updated"),
        $consumerTotal: $("#consumer-list-total"),
        $consumerTableBody: $("#consumer-table-body"),
        $producerUpdated: $("#producer-list-updated"),
        $producerTotal: $("#producer-list-total"),
        $producerTableBody: $("#producer-table-body"),
    });

    $("#refresh-consumers-btn").click(refreshConsumers);
    $("#refresh-producers-btn").click(refreshProducers);

    refreshConsumers();
    refreshProducers();
}

function refreshConsumers() {
    $.get(App.contextPath + "/api/manager/consumers", handleConsumers);
}

function handleConsumers(data) {
    var table = App.manager.$consumerTableBody,
        template = App.manager.consumerTemplate;
    table.html('');
    _.each(data.content, function(c) {
        c.contextPath = App.contextPath;
        c.lastUsedTimeText = c.lastUsedTime < 0 ? 'never' : moment(c.lastUsedTime).fromNow();
        c.lastMessageTimeText = c.lastMessageTime < 0 ? 'never' : moment(c.lastMessageTime).fromNow();
        table.append(template(c));
        $('#delete-' + c.consumerGroupId + '-btn').click(function() { disposeConsumer(c.consumerGroupId); });
    });
}

function disposeConsumer(consumerId) {
    $.ajax({
        type: "DELETE",
        url: App.contextPath + "/api/manager/consumers/" + consumerId,
        success: refreshConsumers
    });
}

function refreshProducers() {
    $.get(App.contextPath + "/api/manager/producers", handleProducers);
}

function handleProducers(data) {
    console.log(data);
    var table = App.manager.$producerTableBody,
        template = App.manager.producerTemplate;
    table.html('');
    _.each(data.content, function(p) {
        p.contextPath = App.contextPath;
        p.lastUsedTimeText = p.lastUsedTime < 0 ? 'never' : moment(p.lastUsedTime).fromNow();
        table.append(template(p));
        $('#delete-' + p.id + '-btn').click(function() { disposeProducer(p.id); });
    });
}

function disposeProducer(producerId) {
    $.ajax({
        type: "DELETE",
        url: App.contextPath + "/api/manager/producers/" + producerId,
        success: refreshProducers
    });
}
