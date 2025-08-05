select 
  floor((row_number() over (order by checkoutparticipant.`id`) - 1) / {total_rows_in_batches}) + 1 as `batch_number`
, formfieldanswer.`checkoutParticipantId` as `participant_id`
, cast(date_add(checkoutparticipant.`createdAt`, interval -3 hour) as date) as `data_compra`
, eventcomplement.`globalEvent` as `edicao`
, event.`title` as `evento`
, formfieldanswer.`fieldId` as `field_id`
, formFieldsPlaceholderParsed.`placeholder` as `pergunta_cadastrada`
, formFieldsPlaceholderParsed.`placeholderParsed` as `pergunta_tratada`
, formfieldanswer.`answer` as `cep`

from formfieldanswer
    left join formFieldsPlaceholderParsed               on formFieldsPlaceholderParsed.`fieldId` collate 'utf8mb4_0900_ai_ci' = formfieldanswer.`fieldId` collate 'utf8mb4_0900_ai_ci'
    left join checkoutparticipant                       on checkoutparticipant.`id` = formfieldanswer.`checkoutParticipantId`
    left join checkouteventticketbatchprice             on checkouteventticketbatchprice.`id` = checkoutparticipant.`checkoutEventTicketBatchPriceId`
    left join eventticketbatchprice                     on eventticketbatchprice.`id` = checkouteventticketbatchprice.`eventTicketBatchPriceId`
    left join eventticketbatch                          on eventticketbatch.`id` = eventticketbatchprice.`ticketBatchId`
    left join eventticket                               on eventticket.`id` = eventticketbatch.`ticketId`
    left join event                                     on event.`id` = eventticket.`eventId`
    left join eventcomplement                           on eventcomplement.`eventId` = event.`id`
    left join eventticketcomplement                     on eventticketcomplement.`ticketId` = eventticket.`id`
    left join checkoutsession                           on checkoutsession.`id` = checkoutparticipant.`sessionId`
    left join checkoutorderticketpartialcancel          on checkoutorderticketpartialcancel.`participantId` = checkoutparticipant.`id`
    
where 1=1
  and eventcomplement.`globalEvent` = "{edicao}"
  and cast(date_add(checkoutparticipant.`createdAt`, interval -3 hour) as date) between "{data_compra_ini}" and "{data_compra_fini}"
  and checkoutsession.`status` = 'Paid'
  and checkoutorderticketpartialcancel.`reason` is null
  and formFieldsPlaceholderParsed.`placeholderParsed` = 'CEP'
  and formfieldanswer.`answer` <> 'true'
  and formfieldanswer.`answer` <> 'null'

limit {limit_max_rows}
;