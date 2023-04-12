package servlets;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import constant.Constant;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import db.MongoManager;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

@WebServlet(name = "StatsServlet", value = "/stats")
public class StatsServlet extends HttpServlet {

    private MongoCollection<Document> collection;

    @Override
    public void init() throws ServletException {
        collection = MongoManager.getClient().getDatabase(Constant.DB_NAME).getCollection(Constant.COLLECTION_STATS);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType(Constant.RES_TYPE);

        String url = request.getPathInfo();

        // validate url
        if (url == null || url.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write(Constant.INVALID_INPUT);
            return;
        }
        String[] urlParts = url.split("/");
        if (urlParts.length < 2) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write(Constant.INVALID_INPUT);
            return;
        }

        int id = Integer.parseInt(urlParts[1]);
        Document doc = collection.find(eq("_id", id)).first();

        if (doc == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(doc.toJson());
            response.getWriter().flush();
        }
    }
}
