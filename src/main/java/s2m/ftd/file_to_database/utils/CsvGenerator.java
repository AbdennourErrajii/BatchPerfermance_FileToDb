package s2m.ftd.file_to_database.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Locale;
import java.util.Random;

public class CsvGenerator {
    public static void generateCsv(String filePath, int rows) throws IOException {
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.append("transactionId,carteId,dateTransaction,montant,devise,merchant,pays,typeCarte,statut,canal,sourceCompte,destinationCompte\n");

            Random rand = new Random();
            String[] devises = {"EUR", "USD", "MAD"};
            String[] merchants = {"Amazon", "Netflix", "Spotify", "Carrefour", "Uber"};
            String[] pays = {"FR", "US", "MA", "DE", "ES"};
            String[] typesCarte = {"VISA", "MASTERCARD", "AMEX"};
            String[] statuts = {"SUCCES", "ECHEC", "PENDING"};
            String[] canaux = {"EN_LIGNE", "ATM", "MOBILE"};
            int elementsPerGroup = rows / 4;
            for (int i = 0; i < rows; i++) {
                Long transactionId = (long) (i + 1);
                String carteId = "C" + (100000000 + rand.nextInt(900000000));
                LocalDate dateTransaction = LocalDate.now().minusDays(rand.nextInt(60));
                double montant = rand.nextDouble() * 5000;
                String devise = devises[rand.nextInt(devises.length)];
                String merchant = merchants[rand.nextInt(merchants.length)];
                String paysVal = pays[rand.nextInt(pays.length)];
                String typeCarte = typesCarte[rand.nextInt(typesCarte.length)];
                String statut = statuts[rand.nextInt(statuts.length)];
                String canal = canaux[rand.nextInt(canaux.length)];
                String sourceCompte = "SC" + (10000000 + rand.nextInt(90000000));
                String destinationCompte = "DC" + (10000000 + rand.nextInt(90000000));

                writer.append(transactionId.toString()).append(",")
                        .append(carteId).append(",")
                        .append(dateTransaction.toString()).append(",")
                        .append(String.format(Locale.US,"%.2f", montant)).append(",")
                        .append(devise).append(",")
                        .append(merchant).append(",")
                        .append(paysVal).append(",")
                        .append(typeCarte).append(",")
                        .append(statut).append(",")
                        .append(canal).append(",")
                        .append(sourceCompte).append(",")
                        .append(destinationCompte).append("\n");
            }
        }
    }
}