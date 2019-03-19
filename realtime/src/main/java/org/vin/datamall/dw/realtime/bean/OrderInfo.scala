package org.vin.datamall.dw.realtime.bean

case class OrderInfo(
                      area: String,
                      consignee: String,
                      orderComment: String,
                      consigneeTel: String,
                      operateTime: String,
                      orderStatus: String,
                      paymentWay: String,
                      userId: String,
                      imgUrl: String,
                      totalAmount: Double,
                      expireTime: String,
                      deliveryAddress: String,
                      createTime: String,
                      trackingNo: String,
                      parentOrderId: String,
                      outTradeNo: String,
                      id: String,
                      tradeBody: String,
                      province_id: String,
                      var createDate: String,
                      var createHour: String,
                      var createHourMinute: String
                    ) {

}
