package cn.itcast.hotel.mq;

import cn.itcast.hotel.constant.MQConstants;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author kaede
 * @create 2023-02-06
 */

@Component
public class HotelListener {

    @Autowired
    private IHotelService hotelService;

    @RabbitListener(queues = MQConstants.HOTEL_INSERT_QUEUE)
    public void listenHotelInsertOrUpdate(Long id) {
        hotelService.insertById(id);
    }

    @RabbitListener(queues = MQConstants.HOTEL_DELETE_QUEUE)
    public void listenHotelDelete(Long id) {
        hotelService.deleteById(id);
    }

}
