package servlets;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import model.*;
import rmqpool.*;
import constant.*;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.TimeoutException;

@WebServlet(name = "SwipeServlet", value = "/swipe")
public class SwipeServlet extends HttpServlet {

    private Gson gson = new Gson();
    private RMQChannelPool pool;

    @Override
    public void init() throws ServletException {
        super.init();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constant.HOST);
        factory.setPort(Constant.PORT);
        factory.setUsername(Constant.USER);
        factory.setPassword(Constant.PASSWORD);
        try {
            Connection connection = factory.newConnection();
            this.pool = new RMQChannelPool(Constant.POOL_SIZE, new RMQChannelFactory(connection));
        } catch (IOException | TimeoutException e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        processRequest(request, response);
    }

    private void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType(Constant.RES_TYPE);

        String url = request.getPathInfo();

        // validate url
        if (!url.equals(Constant.LEFT) && !url.equals(Constant.RIGHT)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write(Constant.WRONG_URL);
            response.getWriter().flush();
            return;
        }

        StringBuilder sb = new StringBuilder();
        String s;

        // validate request form
        try {
            while ((s = request.getReader().readLine()) != null) {
                sb.append(s);
            }
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write(e.getMessage());
            response.getWriter().flush();
        }

        SwipeDetails detail = this.gson.fromJson(sb.toString(), SwipeDetails.class);

        if (!isValid(detail)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write(Constant.INVALID_INPUT);
            response.getWriter().flush();
            return;
        }

        JsonObject info = new JsonObject();
        boolean userLiked = url.equals(Constant.RIGHT);
        info.addProperty(Constant.USER_LIKED, userLiked);
        info.addProperty(Constant.SWIPER, detail.getSwiper());
        info.addProperty(Constant.SWIPEE, detail.getSwipee());

        Channel channel = pool.borrowObject();
        // exchange is declared in channel pool
        channel.basicPublish(Constant.EXCHANGE_NAME, "", null, info.toString().getBytes());
        pool.returnObject(channel);

        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(Constant.SUCCEED);
        response.getWriter().flush();
    }

    private boolean isValid(SwipeDetails detail) {
        if (detail.getSwiper() == null || detail.getSwipee() == null) return false;
        if (!isBetween(detail.getSwiper(), Constant.SWIPE_MINIMUM, Constant.SWIPER_MAX)) return false;
        if (!isBetween(detail.getSwipee(), Constant.SWIPE_MINIMUM, Constant.SWIPEE_MAX)) return false;
        if (detail.getComment().length() > Constant.COMMENT_MAX) return false;
        return true;
    }

    private boolean isBetween(String str, int start, int end) {
        for (char ch : str.toCharArray()) {
            if (!Character.isDigit(ch)) return false;
        }
        int val = Integer.parseInt(str);
        if (val >= start && val <= end) return true;
        return false;
    }
}
