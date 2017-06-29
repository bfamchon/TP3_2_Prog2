package unip.mappers;

import akka.actor.ActorSelection;
import akka.actor.UntypedAbstractActor;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by baptiste on 08/06/17.
 * Hi
 */
public class Mapper extends UntypedAbstractActor {
    private String name;
    private List<ActorSelection> reducers = new ArrayList<>();


    public Mapper(String name, List<String> urls) {
        this.name = name;
        for (String url : urls) {
            this.reducers.add(getContext().actorSelection(url));
        }
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof String) {
            handleLineReception((String) message);
        }
    }

    private void handleLineReception(String line) {
        System.out.println("Mapper " + this.name + ": handleLineReception()");
        line = line.replaceAll("[^A-Za-z0-9áàâäãåçéèêëíìîïñóòôöõúùûüýÿæœÁÀÂÄÃÅÇÉÈÊËÍÌÎÏÑÓÒÔÖÕÚÙÛÜÝŸÆŒ\\s]", "");
        StringTokenizer st = new StringTokenizer(line, " ");
        String word = st.nextToken();
        while (st.hasMoreTokens()) {
            int index = Math.abs(word.hashCode()) % this.reducers.size();
            this.reducers.get(index).tell(word, self());
            word = st.nextToken();
        }
    }

    @Override
    public void preStart() {
        System.out.println("Mapper " + this.name + ": preStart()");

    }
}
