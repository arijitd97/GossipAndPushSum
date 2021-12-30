// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp
#if INTERACTIVE
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#endif

open System
open System.Collections.Generic
open Akka.FSharp
open Akka.Actor

type MasterMessageTypes = 
    | SetParameters of String
    | ConvergenceCounter

type WorkerMessageTypes =
    | InitialiseNeighbours of IActorRef []
    | StartGossip
    | GossipReceive
    | InitialiseSum of double
    | StartPushSum
    | ComputePushSum of double * double
    | Scheduler
    | SendPushParam of double * double


let system = ActorSystem.Create("Gossip")

let actorClosed = new Dictionary<IActorRef,bool>()      // Dictionary that stores whether a node has converged

let Worker (mailbox: Actor<_>)=
    let mutable neighborArr = [||]
    let mutable rumour = 0
    let mutable masterRef:IActorRef = null

    let mutable sum = 0.0
    let mutable weight = 1.0
    let mutable sumReceived = 0.0
    let mutable weightReceived = 0.0
    let delta = (10.0 ** -10.0)
    let mutable noChange = 0

    let rec loop()= actor{
       let! msg = mailbox.Receive()
       if not actorClosed.[mailbox.Self] then          // Worker Actor works only if it is not closed. This is checked using the actor's corresponding value stored in the global dictionary
        match msg with
        | InitialiseNeighbours narray ->               // Initialises the neighbor array for the particular node 
            neighborArr <- narray
            masterRef <- mailbox.Sender()

        | StartGossip ->                               // Gossip Algorithm
            rumour <- rumour+1
            if rumour=10 then
                masterRef <! ConvergenceCounter
                actorClosed.[mailbox.Self]<-true
            let randNeighbor = neighborArr.[Random().Next(0, neighborArr.Length)]
            randNeighbor <! StartGossip
            mailbox.Self <! StartGossip
            
        | InitialiseSum s ->                           // Initialises the sum variable for Push-Sum
            sum <- s
      
        | StartPushSum ->                              // Push-Sum Algorithm invoked by the scheduler
            let currRatio = sum / weight
            sum <- sumReceived
            weight <- weightReceived
            let newRatio = sum / weight
            sumReceived <- 0.0
            weightReceived <- 0.0           
            let change = abs(newRatio - currRatio)
            if change < delta then
                noChange <- noChange + 1
            else 
                noChange <- 0
            if noChange = 3 then
                //printfn "%f" newRatio
                actorClosed.[mailbox.Self] <- true
                masterRef <! ConvergenceCounter
            sum <- sum/2.0
            weight <- weight/2.0
            let randNeighbor = neighborArr.[Random().Next(neighborArr.Length)]
            randNeighbor <! SendPushParam(sum,weight)
            mailbox.Self <! SendPushParam(sum,weight)

        | SendPushParam (s, w) ->                      // Stores the sum and weights received in a round
            sumReceived <- sumReceived + s
            weightReceived <- weightReceived + w

        | Scheduler ->                                 // Scheduler which invokes the worker actor(node) in intervals(Signifies changing rounds)
            mailbox.Self <! SendPushParam (sum , weight)
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(50.0), mailbox.Self, StartPushSum)
        return! loop()
    }
    loop()

let Master(mailbox: Actor<'a>) =
    let mutable nodes = int (string (Environment.GetCommandLineArgs().[2]))
    let topology = string (Environment.GetCommandLineArgs().[3])
    let algorithm = string (Environment.GetCommandLineArgs().[4])
    let mutable arrSize = 0
    let mutable count = 0
    let stopWatch = Diagnostics.Stopwatch()

    let rec loop()= actor{
        let! msg = mailbox.Receive()
        match msg with
        | SetParameters _ ->
            //printfn "In Master"
            printfn "Topology selected is %s" topology
            if topology = "3D" || topology = "Imp3D" then           // For creating the 3D grid number of nodes needs to be a cube of a number
                arrSize <- float nodes ** (1.0/3.0) |> round |>int
                nodes <- float arrSize ** 3.0 |> int
                printfn "New nodes for creating 3D lattice is %i" nodes
            let nodeArray = Array.zeroCreate(nodes)
            for i in [0..(nodes-1)] do
                let actorSpace:string = "key"+string(i)
                let gossipWorkerRef = spawn system (actorSpace) Worker
                nodeArray.[i] <- gossipWorkerRef
                actorClosed.Add(nodeArray.[i],false)
            match topology with
            | "Full" ->
                for i in [0..(nodes-1)] do
                    let mutable neighborArr = [||]
                    for j in [0..(nodes-1)] do
                        if i<>j then 
                            neighborArr <- Array.append neighborArr [|nodeArray.[j]|]
                    nodeArray.[i] <! InitialiseNeighbours(neighborArr)

            | "Line" ->
                for i in [0..(nodes-1)] do
                    let mutable neighborArr = [||]
                    if i=0 then
                        neighborArr <- Array.append neighborArr [|nodeArray.[i+1]|]
                    elif i=(nodes-1) then
                        neighborArr <- Array.append neighborArr [|nodeArray.[i-1]|]
                    else
                        neighborArr <- Array.append neighborArr [|nodeArray.[i-1];nodeArray.[i+1]|]
                    nodeArray.[i] <! InitialiseNeighbours(neighborArr)

            | "3D" ->
                for i in [0 .. (arrSize - 1)] do
                    for j in [0 .. (arrSize - 1)] do
                        for k in [0 .. (arrSize - 1)] do
                            let mutable neighborArr = [||]
                            if (k-1)>=0 then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + j*arrSize + k-1]|]
                            if (k+1)<arrSize then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + j*arrSize + k+1]|]
                            if (j-1)>=0 then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + (j-1)*arrSize + k]|]
                            if (j+1)<arrSize then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + (j+1)*arrSize + k]|]
                            if (i-1)>=0 then
                                neighborArr <- Array.append neighborArr [|nodeArray.[(i-1)*arrSize*arrSize + j*arrSize + k]|]
                            if (i+1)<arrSize then
                                neighborArr <- Array.append neighborArr [|nodeArray.[(i+1)*arrSize*arrSize + j*arrSize + k]|]

                            nodeArray.[i*arrSize*arrSize + j*arrSize + k] <! InitialiseNeighbours(neighborArr)

            | "Imp3D" ->
                for i in [0 .. (arrSize - 1)] do
                    for j in [0 .. (arrSize - 1)] do
                        for k in [0 .. (arrSize - 1)] do
                            let mutable neighborArr = [||]
                            if (k-1)>=0 then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + j*arrSize + k-1]|]
                            if (k+1)<arrSize then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + j*arrSize + k+1]|]
                            if (j-1)>=0 then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + (j-1)*arrSize + k]|]
                            if (j+1)<arrSize then
                                neighborArr <- Array.append neighborArr [|nodeArray.[i*arrSize*arrSize + (j+1)*arrSize + k]|]
                            if (i-1)>=0 then
                                neighborArr <- Array.append neighborArr [|nodeArray.[(i-1)*arrSize*arrSize + j*arrSize + k]|]
                            if (i+1)<arrSize then
                                neighborArr <- Array.append neighborArr [|nodeArray.[(i+1)*arrSize*arrSize + j*arrSize + k]|]
                            let randomNode = nodeArray.[Random().Next(0, nodeArray.Length)]
                            neighborArr <- Array.append neighborArr [|randomNode|]
                            nodeArray.[i*arrSize*arrSize + j*arrSize + k] <! InitialiseNeighbours(neighborArr)

            | _ -> 
                printfn "Invalid topology"

            if algorithm = "Gossip" then
                printfn "Algorithm selected is Gossip"
                let startNode = nodeArray.[Random().Next(0, nodeArray.Length)]
                stopWatch.Start()
                startNode <! StartGossip
                

            if algorithm = "Push" then
                printfn "Algorithm selected is Push-Sum"
                let startNode = nodeArray.[Random().Next(0, nodeArray.Length)]
                for i in [0..(nodes-1)] do
                    nodeArray.[i] <! InitialiseSum(double (i+1))
                stopWatch.Start()
                for nA in nodeArray do
                    nA <! Scheduler


        | ConvergenceCounter ->                     // Keeps track of converged nodes
            count <- count + 1
            printfn "%i nodes have converged." count
            if count = nodes then
                stopWatch.Stop()
                printfn "All nodes have converged! Convergence time for %s in %s is %f ms." topology algorithm stopWatch.Elapsed.TotalMilliseconds
                Environment.Exit(0)

        return! loop()
    }
    loop()

let masterRef = spawn system "MasterSpace" Master

masterRef <! SetParameters "Let's Start!!"

Console.ReadLine() |> ignore