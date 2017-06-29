package unip.master;

import akka.actor.*;

import java.io.*;
/**
 * Created by baptiste on 08/06/17.
 * Hi
 */
public class Master extends UntypedAbstractActor {

    /**
     * Read a file and dispatch line by line to different mappers
     */
    private void dispatchFile(File text) {
        System.out.println("Master: dispatchFile()");
        try (BufferedReader br = new BufferedReader(new FileReader(text))) {
            String line;
            int index = 0;
            while ((line = br.readLine()) != null) {
                String url = "akka.tcp://MySystem2@localhost:2552/user/map" + index % 3;
                ActorSelection map = getContext().actorSelection(url);
                map.tell(line, self());
                index++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleFileReception(String filePath) {
        System.out.println("Master: handleFileReception()");
        File text = new File(filePath);
        if (text.exists() && !text.isDirectory()) {
            System.out.println("Dispatch file to the mappers");
            dispatchFile(text);
        }
    }

    /**
     * Handle AkkaProcess message
     * @param message AkkaProcess send us a FileURL or an Actor
     * @throws InterruptedException exceptions
     */
    @Override
    public void onReceive(Object message) throws InterruptedException {
        if ( message instanceof String ) {
            System.out.println("Master: onReceive(String)");
            handleFileReception((String) message);
        }
    }

    @Override
    public void preStart() {
        System.out.println("Master: preStart()");
    }
}
