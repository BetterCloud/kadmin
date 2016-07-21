function initMain() {
    console.log(App);
    if (!(App.pageLoaded && App.mainTemplateLoaded && App.consumerTemplateLoaded && App.producerTemplateLoaded)) {
        // not ready yet
        return;
    }
    App.manager = _.extend(App.manager, {
        $consumerUpdated: $("#consumer-list-updated"),
        $consumerTotal: $("#consumer-list-total"),
        $consumerTableBody: $("#consumer-table-body"),
    });

    $("#refresh-consumers-btn").click(refreshConsumers);
    // $("#refresh-producers-btn").click(refreshProducers);

    refreshConsumers();
}

function refreshConsumers() {
    $.get(App.contextPath + "/api/manager/consumers", handleConsumers);
}

function handleConsumers(data) {
    console.log(data);
    var table = App.manager.$consumerTableBody,
        template = App.manager.consumerTemplate;
    table.html('');
    _.each(data.content, function(c) {
        c.lastUsedTimeText = c.lastUsedTime < 0 ? 'never' : moment(c.lastUsedTime).fromNow();
        c.lastMessageTimeText = c.lastMessageTime < 0 ? 'never' : moment(c.lastMessageTime).fromNow();
        table.append(template(c));
    });
}
