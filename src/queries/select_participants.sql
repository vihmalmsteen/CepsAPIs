select 
  floor((row_number() over (order by checkoutparticipant.`id`) - 1) / {total_rows_in_batches}) + 1 as `batch_number`
, checkoutparticipant.`id` as `itemID`
, checkoutsession.`id` as `pedidoID`
, cast(date_add(checkoutsession.`createdAt`, interval -3 hour) as date) as `data de compra`
, upper(concat(checkoutparticipant.`name`, ' ', checkoutparticipant.`surname`)) as `participante`
, ifnull(trade_new_item.`old_item_id`, trade_old_item.`new_item_id`) as `itemId correlacionado`
, ifnull(trade_new_item.`participante_correlacionado`, trade_old_item.`participante_correlacionado`) as `participante correlacionado`
, eventcomplement.`globalevent` as `edição`
, event.`title` as `evento`
, eventticketcomplement.`nameParsed` as `prova`
, eventticket.`name` as `ticket`
, eventticketbatchpricetype.`name` as `tipo do ticket`
, checkoutparticipant.`trackingCode` as `rastreio do ticket`
, checkoutparticipant.`coupon` as `cupom`
, case when isnull(checkoutpayment.`paymentOption`) then 'Cortesia'
       when checkoutpayment.`paymentOption` = 'Boleto' then 'Boleto'
       when checkoutpayment.`paymentOption` = 'CreditCard' then 'Cartão'
       when checkoutpayment.`paymentOption` = 'Pix' then 'Pix'
       else checkoutpayment.`paymentOption`
       end as `forma de pgto`
, checkoutparticipant.`email`
, checkoutparticipant.`phoneNumber` as `telefone`
, checkoutparticipant.`cpf` as `CPF`
, checkoutparticipant.`birthDate` as `nascimento`
, year(now()) - year(checkoutparticipant.`birthDate`) as `idade`
, case when year(now()) - year(checkoutparticipant.`birthDate`) between  0 and 20 then 'até 20 anos'
       when year(now()) - year(checkoutparticipant.`birthDate`) between 20 and 30 then 'de 21 a 30 anos'
       when year(now()) - year(checkoutparticipant.`birthDate`) between 30 and 40 then 'de 31 a 40 anos'
       when year(now()) - year(checkoutparticipant.`birthDate`) between 40 and 50 then 'de 41 a 50 anos'
       when year(now()) - year(checkoutparticipant.`birthDate`) between 50 and 60 then 'de 51 a 60 anos'
       when year(now()) - year(checkoutparticipant.`birthDate`) > 60 then 'acima de 60 anos'
       when year(now()) - year(checkoutparticipant.`birthDate`) <  0 then 'até 20 anos'
       else null end as `faixa etária`
, camisas.`camisa` as `camisa`
, if(checkoutsummary.`ticketInsurance` = 0, null, 'X') as `reembolsável`
, agendamentos.`day` as `dia da retirada`
, agendamentos.`hour` as `hora da retirada`

from checkoutparticipant
    left join checkouteventticketbatchprice             on checkouteventticketbatchprice.`id` = checkoutparticipant.`checkoutEventTicketBatchPriceId`
    left join eventticketbatchprice                     on eventticketbatchprice.`id` = checkouteventticketbatchprice.`eventTicketBatchPriceId`
    left join eventticketbatchpricetype                 on eventticketbatchpricetype.`id` = eventticketbatchprice.`typeId`
    left join eventticketbatch                          on eventticketbatch.`id` = eventticketbatchprice.`ticketBatchId`
    left join eventticket                               on eventticket.`id` = eventticketbatch.`ticketId`
    left join eventticketcomplement                     on eventticketcomplement.`ticketId` = eventticket.`id`
    left join event                                     on event.`id` = eventticket.`eventId`
    left join eventcomplement                           on eventcomplement.`eventId` = event.`id`
    left join checkoutsession                           on checkoutsession.`id` = checkoutparticipant.`sessionId`
    left join checkoutpayment                           on checkoutpayment.`sessionId` = checkoutsession.`id`
    left join checkoutsummary                           on checkoutsummary.`sessionId` = checkoutsession.`id`
    left join checkoutorderticketpartialcancel          on checkoutorderticketpartialcancel.`participantId` = checkoutparticipant.`id`
    left join tradecheckoutsession                      on tradecheckoutsession.`participantId` = checkoutparticipant.`id`
    
    left join (
        select
          checkoutparticipant_array_vw.`id` as `participanteId`
        , eventticketstockitemscomplement.`nameParsed` as `camisa`
        from checkoutparticipant_array_vw
            left join eventticketstockitems             on eventticketstockitems.`id` = checkoutparticipant_array_vw.`questionIDs`
            left join eventticketstockitemscomplement   on eventticketstockitemscomplement.`stockitemsid` = eventticketstockitems.`id`
            left join eventticketstock                  on eventticketstock.`id` = eventticketstockitems.`ticketStockId`
        where eventticketstock.`name` in ('Tamanho da Camisa', "Shirt Size")
    ) as camisas on camisas.`participanteId` = checkoutparticipant.`id`

    left join (
        select 
          checkoutparticipant_array_vw.`id` as `participanteId`
        , eventticketstockitemscomplement.`day`
        , eventticketstockitemscomplement.`hour`
        from checkoutparticipant_array_vw
            left join eventticketstockitems             on eventticketstockitems.`id` = checkoutparticipant_array_vw.`questionIDs`
            left join eventticketstockitemscomplement   on eventticketstockitemscomplement.`stockitemsid` = eventticketstockitems.`id`
            left join eventticketstock                  on eventticketstock.`id` = eventticketstockitems.`ticketStockId`
        where eventticketstock.`name` in ('Retirada de Kit', 'Kit Pickup')
    ) as agendamentos on agendamentos.`participanteId` = checkoutparticipant.`id`

    left join (
        select 
          tradecheckoutsession.`originalParticipantId` as `new_item_id`
        , tradecheckoutsession.`participantId` as `old_item_id`
        , tradecheckoutsession.`transferValue` as `troca_valor`
        , upper(concat(checkoutparticipant.`name`, ' ', checkoutparticipant.`surname`)) as `participante_correlacionado`
        from tradecheckoutsession
            left join checkoutparticipant on checkoutparticipant.`id` = tradecheckoutsession.`originalParticipantId`
        where tradecheckoutsession.`status` = 'Paid'
    ) as trade_old_item on trade_old_item.`old_item_id` = checkoutparticipant.`id`

    left join (
        select 
          tradecheckoutsession.`originalParticipantId` as `new_item_id`
        , tradecheckoutsession.`participantId` as `old_item_id`
        , tradecheckoutsession.`transferValue` as `troca_valor`
        , upper(concat(tradecheckoutsession.`participantName`, ' ', tradecheckoutsession.`participantSurname`)) as `participante_correlacionado`
        from tradecheckoutsession
        where tradecheckoutsession.`status` = 'Paid'
    ) as trade_new_item on trade_new_item.`new_item_id` = checkoutparticipant.`id`

where 1=1
  and eventcomplement.`globalEvent` = '{edicao}'
  and event.`id` <> 211  -- loja de servicos
  and checkoutsession.`status` = 'Paid'
  and checkoutorderticketpartialcancel.`reason` is null
  and cast(date_add(checkoutsession.`createdAt`, interval -3 hour) as date) between '{data_compra_ini}' and '{data_compra_fini}'

limit {limit_max_rows}
;