use tokio::sync::{mpsc, oneshot};

use crate::*;

// ##################################################### //
// ################### ACTOR BACKEND ################### //
// ##################################################### //

struct Booster {
   receiver: mpsc::Receiver<BoosterMessage>,

    underlings: Vec<String>,
	underling_grades: Vec<f64>,
    brightspace: Option<BrightspaceHandle>, // Brightspace Actor's handle

}

#[derive(Debug)]
enum BoosterMessage {
    ProcessStudentDump { students: Vec<String> },
    ProcessGradeDump { grades: Vec<f64> },
    CountNumberFailingStudents { reply_to: oneshot::Sender<usize> },
    GetAllStudentGrades { reply_to: oneshot::Sender<Vec<f64>> },
    GetAllStudentNames { reply_to: oneshot::Sender<Vec<String>> },
    SetUnderlingGrade { name: String, grade: f64 },
    SetBrightspace { brightspace_handle: BrightspaceHandle },
    BoostUnderlingGrade { name: String, grade: f64, reply_to: oneshot::Sender<()> },
}


impl Booster {
	fn new(receiver: mpsc::Receiver<BoosterMessage>) -> Self {
        Booster {
            receiver: receiver,
            underlings: Vec::new(),
            underling_grades: Vec::new(),
            brightspace: None,
        }
    }

    async fn handle_message(&mut self, msg: BoosterMessage) {
        println!("[Actor] Booster is running handle_message() with new BoosterMessage: {:?}", msg);
        match msg {
            BoosterMessage::ProcessStudentDump { students } => self.underlings = students,
            BoosterMessage::ProcessGradeDump { grades } => self.underling_grades = grades,
            BoosterMessage::CountNumberFailingStudents { reply_to } => {
                let count_failed = self.underling_grades
                    .iter()
                    .filter(|grade| **grade < 60.0)
                    .count();

                let _ = reply_to.send(count_failed);
            },
            BoosterMessage::GetAllStudentNames { reply_to } => {
                let _ = reply_to.send(self.underlings.clone());
            }
            BoosterMessage::SetUnderlingGrade { name, grade } => {
                println!("[ACTOR]: Booster setting {} grade to {}", name, grade);
                let found_index: Option<usize> = self.underlings.iter().position(|n| *n == name);
                if let Some(ind) = found_index {
                    self.underling_grades[ind] = grade;
                }
            }
            BoosterMessage::BoostUnderlingGrade { name, grade, reply_to } => {
                println!("[ACTOR]: Booster boosting {} grade to {}", name, grade);

                let found_index: Option<usize> = self.underlings.iter().position(|n| *n == name);
                if let Some(ind) = found_index {
                    self.underling_grades[ind] = grade;
                }

                if let Some(brightspace) = &self.brightspace {
                    brightspace.enter_student_grades_into_brightspace(self.underling_grades.clone()).await;
                    brightspace.report_all_students_and_grades_to_admin().await;
                }

                let _ = reply_to.send(());
            }
            BoosterMessage::GetAllStudentGrades { reply_to } => {
                let _ = reply_to.send(self.underling_grades.clone());
            }
            BoosterMessage::SetBrightspace { brightspace_handle } => self.brightspace = Some(brightspace_handle),
        };
    }
}


// ###################################################### //
// ################### ACTOR FRONTEND ################### //
// ###################################################### //

async fn run_booster_actor(mut actor: Booster) {
    println!("[run_booster_actor()]: is blocking until a BoosterMessage is received...");
    while let Some(msg) = actor.receiver.recv().await {
        println!("\n[run_booster_actor()]: received a new BoosterMessage and calling handle_message()...");
        actor.handle_message(msg).await;
    }
}

#[derive(Clone, Debug)]
pub struct BoosterHandle {
    sender: mpsc::Sender<BoosterMessage>,
}

impl BoosterHandle {
    pub async fn boost_grade(&self, name: String, grade: f64) {
        let msg: BoosterMessage = BoosterMessage::SetUnderlingGrade { name, grade };
        let _ = self.sender.send(msg).await;
    }

    pub async fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = Booster::new(receiver);
        tokio::spawn(run_booster_actor(actor));

        BoosterHandle {
            sender,
        }
    }

    pub async fn boost_underling_grade(&self, name: String, grade: f64) {
        let (tx, rx) = oneshot::channel();
        let msg: BoosterMessage = BoosterMessage::BoostUnderlingGrade {
            name,
            grade,
            reply_to: tx,
        };
        let _ = self.sender.send(msg).await;
        let _ = rx.await;
    }

    pub async fn set_brightspace(&self, brightspace_handle: BrightspaceHandle) {
        let msg: BoosterMessage = BoosterMessage::SetBrightspace { brightspace_handle };
        let _ = self.sender.send(msg).await;
    }

    pub async fn submit_student_names(&self, students: Vec<String>) {
        let msg = BoosterMessage::ProcessStudentDump { students };
        let _ = self.sender.send(msg).await;
    }

    pub async fn submit_student_grades(&self, grades: Vec<f64>) {
        let msg = BoosterMessage::ProcessGradeDump { grades };
        let _ = self.sender.send(msg).await;
    }

    pub async fn count_number_of_failing_students(&self) -> usize {
        let (tx, rx) = oneshot::channel();

        let msg = BoosterMessage::CountNumberFailingStudents { reply_to: tx };
        let _ = self.sender.send(msg).await;

        rx.await.unwrap_or(0)
    }

    pub async fn get_all_student_names(&self) -> Vec<String> {
        let (tx, rx) = oneshot::channel();

        let msg = BoosterMessage::GetAllStudentNames { reply_to: tx };
        let _ = self.sender.send(msg).await;

        rx.await.unwrap_or_default()
    }

    pub async fn get_all_student_grades(&self) -> Vec<f64> {
        let (tx, rx) = oneshot::channel();

        let msg = BoosterMessage::GetAllStudentGrades { reply_to: tx };
        let _ = self.sender.send(msg).await;

        rx.await.unwrap_or_default()
    }
}
