package servlets;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;

import com.mongodb.client.*;
import constant.Constant;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import db.MongoManager;
import org.bson.Document;
import org.bson.conversions.Bson;

@WebServlet(name = "MatchesServlet", value = "/matches")
public class MatchesServlet extends HttpServlet {

    private MongoCollection<Document> collection;

    @Override
    public void init() throws ServletException {
        collection = MongoManager.getClient().getDatabase(Constant.DB_NAME).getCollection(Constant.COLLECTION_MATCHES);
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

        response.setStatus(HttpServletResponse.SC_OK);

        int id = Integer.parseInt(urlParts[1]);
        Bson projectionFields = slice("matchList", 100);
        Document doc = collection.find(eq("_id", id)).projection(projectionFields).first();

        if (doc == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write(doc.toJson());
            response.getWriter().flush();
        }
    }

}
