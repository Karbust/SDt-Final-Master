import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Master extends UnicastRemoteObject implements MasterClientInterface {
    private int fileCount = 0;
    StorageMasterInterface smi;
    String portaMapper = "2030";
    ArrayList<String> reducersList = new ArrayList<>();

    public Master() throws Exception {
        reducersList.add("2040");
        reducersList.add("2041");
        reducersList.add("2042");

        Thread t = (new Thread(() -> {
            for (String reducer : reducersList) {
                RMIReducer.main(new String[]{reducer});
            }
            /*RMIReducer.main(new String[]{"2040"});
            RMIReducer.main(new String[]{"2041"});
            RMIReducer.main(new String[]{"2042"});*/
            RMIMapper.main(new String[]{portaMapper}); //in use
        }));
        t.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        smi = (StorageMasterInterface) Naming.lookup("rmi://localhost:2025/storage");

        Thread t1 = (new Thread(() -> {
            try {
                while(true) {
                    for (String reducerId : reducersList) {
                        try {
                            Naming.lookup("rmi://localhost:" + reducerId + "/reducer");
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                            RMIReducer.main(new String[]{reducerId});
                            Boolean flag = smi.hasReducerFinishedJob(portaMapper, "Reducer" + reducerId);
                            if (flag == null || !flag) {
                                ReducerMasterInterface rmi = (ReducerMasterInterface) Naming.lookup("rmi://localhost:" + reducerId + "/reducer");
                                rmi.calculateStatistics(portaMapper, reducerId, fileCount);
                            }
                        }
                    }
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        t1.start();

        Thread t2 = (new Thread(() -> {
            try {
                while(true) {
                    try {
                        Naming.lookup("rmi://localhost:" + portaMapper + "/mapper");
                    } catch (Exception e) {
                        System.out.println("1 - " + e.getMessage());
                        RMIMapper.main(new String[]{portaMapper});
                        Boolean flag = smi.hasMapperFinishedJob(portaMapper);
                        if (!flag) {
                            MapperMasterInterface mmi = (MapperMasterInterface) Naming.lookup("rmi://localhost:" + portaMapper + "/mapper");
                            mmi.calculateCombinations(2, fileCount, reducersList);
                        }
                    }
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        t2.start();
    }

    private int getNumberOfValidCombos() throws Exception {
        ConcurrentHashMap<String, MapperInfo> mapa  = smi.getCombinations();
        if (mapa == null) return -1;

        MapperInfo mInfo = mapa.get(portaMapper);
        if (mInfo == null) return -1;

        int countValidos = 0;
        for (Map.Entry<String, ReducerInfo> entry : mInfo.reducersInfo.entrySet()) {
            ReducerInfo reducer = entry.getValue();
            if (!reducer.status) return -1;

            List<ProcessCombinationModel> combinations = reducer.PCMs;
            for (ProcessCombinationModel pcm : combinations) {
                if (pcm.percentage > 0.5) {
                    countValidos++;
                }
            }
        }

        return countValidos;
    }

    @Override
    public void task(int fileCount) throws Exception {
        this.fileCount = fileCount;

        try {
            MapperMasterInterface mmi = (MapperMasterInterface) Naming.lookup("rmi://localhost:2030/mapper");
            mmi.calculateCombinations(2, fileCount, reducersList);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.out.println("2 - " + e.getMessage());
        }
    }

    @Override
    public int checkTaskResult() throws Exception {
        return getNumberOfValidCombos();
    }
}
