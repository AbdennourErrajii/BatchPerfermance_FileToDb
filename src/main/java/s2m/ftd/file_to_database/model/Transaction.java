package s2m.ftd.file_to_database.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDate;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transaction{
    @Id
    private Long transactionId;
    private Long groupe;
    private String carteId;
    private LocalDate dateTransaction;
    private double montant;
    private String devise;
    private String merchant;
    private String pays;
    private String typeCarte;
    private String statut;
    private String canal;
    private String sourceCompte;
    private String destinationCompte;
}
