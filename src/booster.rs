use tokio::sync::{mpsc, oneshot};

use crate::*;

// ##################################################### //
// ################### ACTOR BACKEND ################### //
// ##################################################### //

struct Booster {
   receiver: mpsc::Receiver<BoosterMessage>,

    underlings: Vec<String>,
	underling_grades: Vec<f64>,
}

#[derive(Debug)]
enum BoosterMessage {
	ProcessStudentDump { students: Vec<String> },
	ProcessGradeDump { grades: Vec<f64> },
    CountNumberFailingStudents { reply_to: oneshot::Sender<usize> },
    GetAllStudentGrades { reply_to: oneshot::Sender<Vec<f64>> },
    GetAllStudentNames { reply_to: oneshot::Sender<Vec<String>> },
}


impl Booster {
	fn new(receiver: mpsc::Receiver<BoosterMessage>) -> Self {
        // TODO
        todo!()
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

            BoosterMessage::GetAllStudentGrades { reply_to } => {
                let _ = reply_to.send(self.underling_grades.clone());
            }
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
    todo!()
}

#[derive(Clone, Debug)]
pub struct BoosterHandle {
    sender: mpsc::Sender<BoosterMessage>,
}

impl BoosterHandle {
    pub async fn new() -> Self {
        // TODO
        todo!()
    }

    // TODO
}
