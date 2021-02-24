import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIMaster {
    public static void main(String[] args) {
        Registry r = null;

        try {
            r = LocateRegistry.createRegistry(2024);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Master master = new Master();
            assert r != null;
            r.bind("master", master);
            System.out.println("Master server ready");
        } catch (Exception e) {
            System.out.println("Master server main " + e.getMessage());
        }
    }
}
